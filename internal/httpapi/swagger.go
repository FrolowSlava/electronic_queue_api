package httpapi

import (
	"io"
	"net/http"

	swaggerFiles "github.com/swaggo/files/v2"
)

func swaggerHandler() http.Handler {
	assets := http.StripPrefix("/docs/", http.FileServer(http.FS(swaggerFiles.FS)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/docs/" {
			assets.ServeHTTP(w, r)
			return
		}
		// Своя стартовая страница подключает наш контракт, а JS/CSS встроены в Go-бинарник.
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, swaggerHTML)
	})
}

const swaggerHTML = `<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Электронная очередь — API</title>
  <link rel="stylesheet" href="./swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="./swagger-ui-bundle.js"></script>
  <script>
    window.onload = function () {
      SwaggerUIBundle({url: "/openapi.yaml", dom_id: "#swagger-ui", deepLinking: true,
        presets: [SwaggerUIBundle.presets.apis], validatorUrl: null});
    };
  </script>
</body>
</html>
`
