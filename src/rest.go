package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

func RunRESTServer() {
	// Register probe metrics before starting the server
	registerProbeMetrics()

	// Initializing fiber
	app := fiber.New(fiber.Config{
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			// Status code defaults to 500
			code := fiber.StatusInternalServerError

			// Retrieve the custom status code if it's a *fiber.Error
			var e *fiber.Error
			if errors.As(err, &e) {
				code = e.Code
			}

			// Sending Response
			return c.Status(code).SendString(err.Error())
		},
	})

	// Middlewares
	app.Use(recover.New(recover.Config{
		EnableStackTrace: true,
	}))

	// Routes
	app.Get("/health", func(c *fiber.Ctx) error {
		c.SendString("OK")
		return nil
	})

	// Add Prometheus metrics endpoint
	// This creates an adapter between Fiber (FastHTTP) and the Prometheus HTTP handler
	handler := promhttp.HandlerFor(globalRegistry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})

	adapter := fasthttpadaptor.NewFastHTTPHandler(handler)
	app.All("/metrics", func(c *fiber.Ctx) error {
		// Call the standard net/http handler using the adaptor
		adapter(c.Context())
		return nil
	})

	// Starting fiber server
	go func() {
		address := fmt.Sprintf(":%v", Config.RESTServerPort)
		Log.Info().Int("port", Config.RESTServerPort).Msg("Starting server with integrated metrics endpoint")
		err := app.Listen(address)
		if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			Log.Fatal().Err(err).Msg("Failed to start server")
		}
	}()
}
