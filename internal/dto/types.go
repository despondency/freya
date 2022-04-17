package dto

import "github.com/francoispqt/gojay"

type StringSlice []string

func (t *StringSlice) MarshalJSONArray(enc *gojay.Encoder) {
	for _, e := range *t {
		enc.String(e)
	}
}

func (t *StringSlice) IsNil() bool {
	return t == nil
}

func (t *StringSlice) UnmarshalJSONArray(dec *gojay.Decoder) error {
	str := ""
	if err := dec.String(&str); err != nil {
		return err
	}
	*t = append(*t, str)
	return nil
}

type PutRequest struct {
	Key   StringSlice
	Value StringSlice
}

func (pr *PutRequest) UnmarshalJSONObject(dec *gojay.Decoder, key string) error {
	switch key {
	case "k":
		return dec.Array(&pr.Key)
	case "v":
		return dec.Array(&pr.Value)
	}
	return nil
}

func (pr *PutRequest) NKeys() int {
	return 2
}

func (pr *PutRequest) MarshalJSONObject(enc *gojay.Encoder) {
	enc.ArrayKey("k", &pr.Key)
	enc.ArrayKey("v", &pr.Value)
}

func (pr *PutRequest) IsNil() bool {
	return pr == nil
}

type GetRequest struct {
	Keys StringSlice
}

func (gr *GetRequest) UnmarshalJSONObject(dec *gojay.Decoder, key string) error {
	switch key {
	case "k":
		return dec.Array(&gr.Keys)
	}
	return nil
}

func (gr *GetRequest) NKeys() int {
	return 1
}

func (gr *GetRequest) MarshalJSONObject(enc *gojay.Encoder) {
	enc.ArrayKey("k", &gr.Keys)
}

func (gr *GetRequest) IsNil() bool {
	return gr == nil
}

type GetResponse struct {
	Values StringSlice
}

func (grb *GetResponse) UnmarshalJSONObject(dec *gojay.Decoder, key string) error {
	switch key {
	case "v":
		return dec.Array(&grb.Values)
	}
	return nil
}

func (grb *GetResponse) NKeys() int {
	return 1
}

func (grb *GetResponse) MarshalJSONObject(enc *gojay.Encoder) {
	enc.ArrayKey("v", &grb.Values)
}

func (grb *GetResponse) IsNil() bool {
	return grb == nil
}
