package transport

import (
	"fmt"
	"github.com/despondency/freya/internal/dto"
	"github.com/despondency/freya/internal/raft"
	"github.com/francoispqt/gojay"
	"github.com/julienschmidt/httprouter"
	"io"
	"log"
	"net/http"
)

type Rest struct {
	port       int
	raftServer *raft.Server
}

func NewRestTransport(port int, raftServer *raft.Server) *Rest {
	return &Rest{
		port:       port,
		raftServer: raftServer,
	}
}

func (r *Rest) StartTransport() {
	mux := httprouter.New()
	mux.Handle(http.MethodGet, "/get", func(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
		b, err := io.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		req := &dto.GetRequest{}
		err = gojay.UnmarshalJSONObject(b, req)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
		}
		resp, err := r.raftServer.Get(req)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
		}
		b, err = gojay.MarshalJSONObject(resp)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
		}
		writer.WriteHeader(http.StatusOK)
		writer.Header().Set("Content-Type", "application/json")
		writer.Write(b)
	})
	mux.Handle(http.MethodPost, "/put", func(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
		b, err := io.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		req := &dto.PutRequest{}
		err = gojay.UnmarshalJSONObject(b, req)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
		}
		err = r.raftServer.Put(req)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
		}
	})
	log.Fatalln(http.ListenAndServe(fmt.Sprintf(":%d", r.port), mux))
}
