package main

import (
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
)

func RunRESTServer() {
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

	// Starting fiber server
	go func() {
		err := app.Listen(fmt.Sprintf(":%v", Config.RESTServerPort))
		if err != nil {
			Logger.Fatal(err)
		}
	}()
}
