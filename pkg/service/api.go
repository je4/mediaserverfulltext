package service

import (
	"context"
	"fmt"
	"github.com/je4/filesystem/v3/pkg/writefs"
	generic "github.com/je4/genericproto/v2/pkg/generic/proto"
	"github.com/je4/mediaserveraction/v2/pkg/actionCache"
	"github.com/je4/mediaserveraction/v2/pkg/actionController"
	"github.com/je4/mediaserverfulltext/v2/pkg/fulltext"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var Type = "text"
var Params = map[string][]string{
	"fulltext": {},
}

func NewActionService(adClient mediaserverproto.ActionDispatcherClient, host string, port uint32, tikaFulltextUrl string, timeout time.Duration, concurrency, queueSize uint32, refreshErrorTimeout time.Duration, vfs fs.FS, db mediaserverproto.DatabaseClient, logger zLogger.ZLogger) (*fulltextAction, error) {
	_logger := logger.With().Str("rpcService", "fulltextAction").Logger()
	return &fulltextAction{
		actionDispatcherClient: adClient,
		done:                   make(chan bool),
		host:                   host,
		port:                   port,
		refreshErrorTimeout:    refreshErrorTimeout,
		vFS:                    vfs,
		db:                     db,
		logger:                 &_logger,
		fulltextHandler:        fulltext.NewFulltextHandler(tikaFulltextUrl, timeout, logger),
		concurrency:            concurrency,
		queueSize:              queueSize,
	}, nil
}

type fulltextAction struct {
	mediaserverproto.UnimplementedActionServer
	actionDispatcherClient mediaserverproto.ActionDispatcherClient
	logger                 zLogger.ZLogger
	done                   chan bool
	host                   string
	port                   uint32
	refreshErrorTimeout    time.Duration
	vFS                    fs.FS
	db                     mediaserverproto.DatabaseClient
	fulltextHandler        *fulltext.FulltextHandler
	concurrency            uint32
	queueSize              uint32
}

func (ia *fulltextAction) Start() error {
	actionParams := map[string]*generic.StringList{}
	for action, params := range Params {
		actionParams[action] = &generic.StringList{
			Values: params,
		}
	}
	go func() {
		for {
			waitDuration := ia.refreshErrorTimeout
			if resp, err := ia.actionDispatcherClient.AddController(context.Background(), &mediaserverproto.ActionDispatcherParam{
				Type:        Type,
				Actions:     actionParams,
				Host:        &ia.host,
				Port:        ia.port,
				Concurrency: ia.concurrency,
				QueueSize:   ia.queueSize,
			}); err != nil {
				ia.logger.Error().Err(err).Msg("cannot add controller")
			} else {
				if resp.GetResponse().GetStatus() != generic.ResultStatus_OK {
					ia.logger.Error().Err(err).Msgf("cannot add controller: %s", resp.GetResponse().GetMessage())
				} else {
					waitDuration = time.Duration(resp.GetNextCallWait()) * time.Second
					ia.logger.Info().Msgf("controller %s:%d added", ia.host, ia.port)
				}
			}
			select {
			case <-time.After(waitDuration):
				continue
			case <-ia.done:
				return
			}
		}
	}()
	return nil
}

func (ia *fulltextAction) GracefulStop() {
	if err := ia.fulltextHandler.Close(); err != nil {
		ia.logger.Error().Err(err).Msg("cannot close fulltext handler")
	}
	actionParams := map[string]*generic.StringList{}
	for action, params := range Params {
		actionParams[action] = &generic.StringList{
			Values: params,
		}
	}
	if resp, err := ia.actionDispatcherClient.RemoveController(context.Background(), &mediaserverproto.ActionDispatcherParam{
		Type:        Type,
		Actions:     actionParams,
		Host:        &ia.host,
		Port:        ia.port,
		Concurrency: ia.concurrency,
	}); err != nil {
		ia.logger.Error().Err(err).Msg("cannot remove controller")
	} else {
		if resp.GetStatus() != generic.ResultStatus_OK {
			ia.logger.Error().Err(err).Msgf("cannot remove controller: %s", resp.GetMessage())
		} else {
			ia.logger.Info().Msgf("controller %s:%d removed", ia.host, ia.port)
		}

	}
	ia.done <- true
}

func (ia *fulltextAction) Ping(context.Context, *emptypb.Empty) (*generic.DefaultResponse, error) {
	return &generic.DefaultResponse{
		Status:  generic.ResultStatus_OK,
		Message: "pong",
		Data:    nil,
	}, nil
}
func (ia *fulltextAction) GetParams(ctx context.Context, param *mediaserverproto.ParamsParam) (*generic.StringList, error) {
	params, ok := Params[param.GetAction()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "action %s::%s not found", param.GetType(), param.GetAction())
	}
	return &generic.StringList{
		Values: params,
	}, nil
}

var isUrlRegexp = regexp.MustCompile(`^[a-z]+://`)

func (ia *fulltextAction) loadFulltext(fulltextPath string) (string, error) {
	fp, err := ia.vFS.Open(fulltextPath)
	if err != nil {
		return "", status.Errorf(codes.NotFound, "cannot open %s: %v", fulltextPath, err)
	}
	defer fp.Close()
	text, err := ia.fulltextHandler.ExtractFulltext(fp, filepath.Base(fulltextPath))
	if err != nil {
		return "", status.Errorf(codes.Internal, "cannot decode %s: %v", fulltextPath, err)
	}
	return text, nil
}

func (ia *fulltextAction) storeFulltext(text string, action string, item *mediaserverproto.Item, itemCache *mediaserverproto.Cache, storage *mediaserverproto.Storage, params actionCache.ActionParams) (*mediaserverproto.Cache, error) {
	itemIdentifier := item.GetIdentifier()
	cacheName := actionController.CreateCacheName(itemIdentifier.GetCollection(), itemIdentifier.GetSignature(), action, params.String(), "txt")
	targetPath := fmt.Sprintf(
		"%s/%s/%s",
		storage.GetFilebase(),
		storage.GetDatadir(),
		cacheName)
	filesize, err := writefs.WriteFile(ia.vFS, targetPath, []byte(text))
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "cannot write %v/%s: %v", ia.vFS, targetPath, err)
	}
	resp := &mediaserverproto.Cache{
		Identifier: &mediaserverproto.ItemIdentifier{
			Collection: itemIdentifier.GetCollection(),
			Signature:  itemIdentifier.GetSignature(),
		},
		Metadata: &mediaserverproto.CacheMetadata{
			Action:   action,
			Params:   params.String(),
			Width:    0,
			Height:   0,
			Duration: 0,
			Size:     int64(filesize),
			MimeType: "text/plain",
			Path:     fmt.Sprintf("%s/%s", storage.GetDatadir(), cacheName),
			Storage:  storage,
		},
	}
	return resp, nil

}

func (ia *fulltextAction) fulltext(item *mediaserverproto.Item, itemCache *mediaserverproto.Cache, storage *mediaserverproto.Storage, params actionCache.ActionParams) (*mediaserverproto.Cache, error) {
	itemIdentifier := item.GetIdentifier()
	cacheItemMetadata := itemCache.GetMetadata()
	ia.logger.Info().Msgf("action %s/%s/%s/%s", itemIdentifier.GetCollection(), itemIdentifier.GetSignature(), "fulltext", params.String())
	itemFulltextPath := cacheItemMetadata.GetPath()
	if !isUrlRegexp.MatchString(itemFulltextPath) {
		itemFulltextPath = fmt.Sprintf("%s/%s", storage.GetFilebase(), strings.TrimPrefix(itemFulltextPath, "/"))
	}
	text, err := ia.loadFulltext(itemFulltextPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot decode %s: %v", itemFulltextPath, err)
	}
	return ia.storeFulltext(text, "fulltext", item, itemCache, storage, params)
}

func (ia *fulltextAction) Action(ctx context.Context, ap *mediaserverproto.ActionParam) (*mediaserverproto.Cache, error) {
	item := ap.GetItem()
	if item == nil {
		return nil, status.Errorf(codes.InvalidArgument, "no item defined")
	}
	itemIdentifier := item.GetIdentifier()
	storage := ap.GetStorage()
	if storage == nil {
		return nil, status.Errorf(codes.InvalidArgument, "no storage defined")
	}
	cacheItem, err := ia.db.GetCache(context.Background(), &mediaserverproto.CacheRequest{
		Identifier: itemIdentifier,
		Action:     "item",
		Params:     "",
	})
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "cannot get cache %s/%s/item: %v", itemIdentifier.GetCollection(), itemIdentifier.GetSignature(), err)
	}
	action := ap.GetAction()
	switch strings.ToLower(action) {
	case "fulltext":
		return ia.fulltext(item, cacheItem, storage, ap.GetParams())
	default:
		return nil, status.Errorf(codes.InvalidArgument, "no action defined")

	}
}
