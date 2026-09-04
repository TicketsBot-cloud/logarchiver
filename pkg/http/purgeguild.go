package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/TicketsBot-cloud/logarchiver/internal"
	"github.com/TicketsBot-cloud/logarchiver/pkg/repository"
	"github.com/TicketsBot-cloud/logarchiver/pkg/repository/model"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
)

func (s *Server) purgeGuildHandler(ctx *gin.Context) {
	guildId, err := strconv.ParseUint(ctx.Param("id"), 10, 64)
	if err != nil {
		ctx.JSON(400, gin.H{
			"success": false,
			"message": "missing guild ID",
		})
		return
	}

	if err := s.RemoveQueue.StartOperation(guildId); err != nil {
		if errors.Is(err, internal.ErrOperationInProgress) {
			ctx.JSON(400, gin.H{
				"success": false,
				"message": "operation already in progress",
			})
		} else {
			ctx.JSON(500, gin.H{
				"success": false,
				"message": err.Error(),
			})
		}
		return
	}

	go func() {
		prefix := fmt.Sprintf("%d/", guildId)
		totalDeleted := 0
		var firstErr error

		// Get all unique bucket IDs (default + any from database)
		bucketIds := make(map[string]struct{})
		bucketIds[s.Config.DefaultBucketId.String()] = struct{}{}

		// Add bucket IDs from database
		var dbObjects []model.Object
		if err := s.store.Tx(context.Background(), func(r repository.Repositories) (err error) {
			dbObjects, err = r.Objects().ListByGuild(context.Background(), guildId)
			return
		}); err != nil {
			s.Logger.Error("Failed to fetch objects from database", zap.Error(err), zap.Uint64("guild", guildId))
			s.RemoveQueue.AddError(guildId, "database:list", err)
			firstErr = err
		} else {
			for _, obj := range dbObjects {
				bucketIds[obj.BucketId.String()] = struct{}{}
			}
		}

		// Delete from each bucket
		for bucketIdStr := range bucketIds {
			s.Logger.Debug("Processing bucket", zap.String("bucket_id", bucketIdStr), zap.Uint64("guild", guildId))

			bucketId, err := uuid.Parse(bucketIdStr)
			if err != nil {
				s.Logger.Error("Failed to parse bucket ID", zap.Error(err), zap.String("bucket_id", bucketIdStr), zap.Uint64("guild", guildId))
				s.RemoveQueue.AddError(guildId, "bucket:"+bucketIdStr, err)
				if firstErr == nil {
					firstErr = err
				}
				continue
			}

			client, err := s.s3Clients.Get(bucketId)
			if err != nil {
				s.Logger.Error("Failed to get S3 client", zap.Error(err), zap.String("bucket_id", bucketIdStr), zap.Uint64("guild", guildId))
				s.RemoveQueue.AddError(guildId, "bucket:"+bucketIdStr, err)
				if firstErr == nil {
					firstErr = err
				}
				continue
			}

			s.Logger.Debug("Listing objects", zap.String("bucket", client.BucketName()), zap.String("prefix", prefix), zap.Uint64("guild", guildId))

			// List all objects with guild prefix
			objectCh := client.Minio().ListObjects(context.Background(), client.BucketName(), minio.ListObjectsOptions{
				Prefix:    prefix,
				Recursive: true,
			})

			// Create channel for RemoveObjects
			objectsCh := make(chan minio.ObjectInfo)

			bucketObjectCount := 0
			listErrCh := make(chan error, 1)
			go func() {
				defer close(objectsCh)

				var listErr error
				for obj := range objectCh {
					if obj.Err != nil {
						s.Logger.Error("Failed to list object", zap.Error(obj.Err), zap.String("bucket_id", bucketIdStr), zap.Uint64("guild", guildId))
						if listErr == nil {
							listErr = obj.Err
						}
						continue
					}
					bucketObjectCount++
					objectsCh <- obj
				}

				listErrCh <- listErr
				s.Logger.Debug("Finished listing objects", zap.Int("count", bucketObjectCount), zap.Uint64("guild", guildId))
			}()

			// Bulk delete
			bucketDeletedCount := 0
			for result := range client.Minio().RemoveObjects(context.Background(), client.BucketName(), objectsCh, minio.RemoveObjectsOptions{}) {
				if result.Err != nil {
					s.Logger.Error("Failed to remove object", zap.Error(result.Err), zap.String("object", result.ObjectName), zap.Uint64("guild", guildId))
					s.RemoveQueue.AddError(guildId, result.ObjectName, result.Err)
					if firstErr == nil {
						firstErr = result.Err
					}
				} else {
					bucketDeletedCount++
					totalDeleted++
				}
			}

			// Reading this also joins the lister.
			if listErr := <-listErrCh; listErr != nil {
				s.RemoveQueue.AddError(guildId, "bucket:"+bucketIdStr, listErr)
				if firstErr == nil {
					firstErr = listErr
				}
			}

			s.RemoveQueue.AddRemovedObject(guildId, fmt.Sprintf("%s (%d objects)", bucketIdStr, bucketObjectCount))

			s.Logger.Debug("Finished deleting from bucket", zap.Int("deleted", bucketDeletedCount), zap.String("bucket_id", bucketIdStr), zap.Uint64("guild", guildId))
		}

		if firstErr == nil {
			if err := s.store.Tx(context.Background(), func(r repository.Repositories) error {
				return r.Objects().DeleteByGuild(context.Background(), guildId)
			}); err != nil {
				s.Logger.Error("Failed to delete objects from database", zap.Error(err), zap.Uint64("guild", guildId))
				s.RemoveQueue.AddError(guildId, "database:delete", err)
				firstErr = err
			}
		} else {
			s.Logger.Warn(
				"Keeping object records because the S3 purge failed; deleting them would strand the transcripts",
				zap.Uint64("guild", guildId),
			)
		}

		if firstErr != nil {
			s.RemoveQueue.SetStatus(guildId, internal.StatusFailed)
			s.Logger.Error("Remove operation failed", zap.Error(firstErr), zap.Uint64("guild", guildId), zap.Int("deleted", totalDeleted))
		} else {
			s.RemoveQueue.SetStatus(guildId, internal.StatusComplete)
			if totalDeleted == 0 {
				s.Logger.Info("Remove operation completed - no transcripts to delete", zap.Uint64("guild", guildId))
			} else {
				s.Logger.Info("Remove operation completed successfully", zap.Uint64("guild", guildId), zap.Int("deleted", totalDeleted))
			}
		}
	}()

	ctx.JSON(http.StatusAccepted, gin.H{
		"success": true,
	})
}
