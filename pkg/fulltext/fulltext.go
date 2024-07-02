package fulltext

import (
	"context"
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/utils/v2/pkg/zLogger"
	"io"
	"net/http"
	"time"
)

func NewFulltextHandler(tikaFulltextUrl string, tikaTimeout time.Duration, logger zLogger.ZLogger) *FulltextHandler {
	return &FulltextHandler{
		tikaFulltextUrl: tikaFulltextUrl,
		tikaTimeout:     tikaTimeout,
		logger:          logger,
	}
}

type FulltextHandler struct {
	tikaFulltextUrl string
	logger          zLogger.ZLogger
	tikaTimeout     time.Duration
}

func (fh *FulltextHandler) Close() error {
	return nil
}

func (fh *FulltextHandler) ExtractFulltext(fp io.Reader, filename string) (string, error) {
	client := &http.Client{}
	ctx, cancel := context.WithTimeout(context.Background(), fh.tikaTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, fh.tikaFulltextUrl, fp)
	if err != nil {
		return "", errors.Wrapf(err, "cannot create tika request - %v", fh.tikaFulltextUrl)
	}
	req.Header.Add("Accept", "text/plain")
	req.Header.Add("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	//req.Header.Add("fileUrl", uri.String())
	tresp, err := client.Do(req)
	if err != nil {
		return "", errors.Wrapf(err, "error in tika request - %v", fh.tikaFulltextUrl)
	}
	defer tresp.Body.Close()
	bodyBytes, err := io.ReadAll(tresp.Body)
	if err != nil {
		return "", errors.Wrapf(err, "error reading body - %v", fh.tikaFulltextUrl)
	}

	if tresp.StatusCode != http.StatusOK {
		return "", errors.New(fmt.Sprintf("status not ok - %v -> %v: %s", fh.tikaFulltextUrl, tresp.Status, string(bodyBytes)))
	}

	/*

		if bodyBytes[0] == '{' {
			bodyBytes = append([]byte{'['}, bodyBytes...)
			bodyBytes = append(bodyBytes, ']')
		}
		meta := make([]map[string]interface{}, 0)
		err = json.Unmarshal(bodyBytes, &meta)
		if err != nil {
			return "", errors.Wrapf(err, "error decoding json - %v", string(bodyBytes))
		}

		if len(meta) > 0 {
			content, ok := meta[0]["X-TIKA:content"]
			if !ok {
				return "", errors.Errorf("no field X-TIKA:content in tika result for %s found", filename)
			}
			if contentString, ok := content.(string); ok {
				return contentString, nil
			}
		}

	*/
	return string(bodyBytes), nil
}
