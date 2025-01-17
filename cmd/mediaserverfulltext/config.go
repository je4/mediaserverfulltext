package main

import (
	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	"github.com/je4/filesystem/v3/pkg/vfsrw"
	loaderConfig "github.com/je4/trustutil/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/zLogger"
	"io/fs"
	"os"
)

type MediaserverFulltextConfig struct {
	LocalAddr               string                 `toml:"localaddr"`
	ClientDomain            string                 `toml:"clientdomain"`
	ResolverAddr            string                 `toml:"resolveraddr"`
	ResolverTimeout         config.Duration        `toml:"resolvertimeout"`
	ResolverNotFoundTimeout config.Duration        `toml:"resolvernotfoundtimeout"`
	TikaFulltextURl         string                 `toml:"tikaFulltextUrl"`
	TikaTimeout             config.Duration        `toml:"tikaTimeout"`
	ServerTLS               loaderConfig.TLSConfig `toml:"servertls"`
	ClientTLS               loaderConfig.TLSConfig `toml:"clienttls"`
	GRPCClient              map[string]string      `toml:"grpcclient"`
	VFS                     map[string]*vfsrw.VFS  `toml:"vfs"`
	Concurrency             uint32                 `toml:"concurrency"`
	QueueSize               uint32                 `toml:"queuesize"`
	Log                     zLogger.Config         `toml:"log"`
}

func LoadMediaserverFulltextConfig(fSys fs.FS, fp string, conf *MediaserverFulltextConfig) error {
	if _, err := fs.Stat(fSys, fp); err != nil {
		path, err := os.Getwd()
		if err != nil {
			return errors.Wrap(err, "cannot get current working directory")
		}
		fSys = os.DirFS(path)
		fp = "mediaserverFulltext.toml"
	}
	data, err := fs.ReadFile(fSys, fp)
	if err != nil {
		return errors.Wrapf(err, "cannot read file [%v] %s", fSys, fp)
	}
	_, err = toml.Decode(string(data), conf)
	if err != nil {
		return errors.Wrapf(err, "error loading config file %v", fp)
	}
	return nil
}
