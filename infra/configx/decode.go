package configx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/viper"
)

// fileDecoders 保存完整源数据用于严格映射，JSON 额外保留数字精度。
type fileDecoders struct {
	values map[string]any
}

func (d *fileDecoders) Decoder(format string) (viper.Decoder, error) {
	var decoder viper.Decoder
	if format == "json" {
		decoder = jsonDecoder{}
	} else {
		var err error
		decoder, err = viper.NewCodecRegistry().Decoder(format)
		if err != nil {
			return nil, err
		}
	}
	return capturedDecoder{decoder: decoder, values: &d.values}, nil
}

type capturedDecoder struct {
	decoder viper.Decoder
	values  *map[string]any
}

func (d capturedDecoder) Decode(data []byte, dst map[string]any) error {
	if err := d.decoder.Decode(data, dst); err != nil {
		return err
	}
	*d.values = dst
	return nil
}

type jsonDecoder struct{}

func (jsonDecoder) Decode(data []byte, dst map[string]any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	// Viper 默认先转 float64，会在映射到 int64/uint64 前丢失大整数精度。
	decoder.UseNumber()
	if err := decoder.Decode(&dst); err != nil {
		return err
	}
	// Decoder 只读取首值；配置文件必须完整，拒绝第二个值及非法尾部。
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("multiple JSON values in config file")
	}
	return nil
}
