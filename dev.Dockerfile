FROM golang:1.23

# Install gopls for golang LSP
RUN go install golang.org/x/tools/gopls@v0.18.0

# Install delve for debugging
RUN go install github.com/go-delve/delve/cmd/dlv@v1.24.0

WORKDIR /app
