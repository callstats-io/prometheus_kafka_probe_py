import logging.config

loggingConfig = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "simple": {
            "format": "%(asctime)s [%(levelname)s] [%(module)s] %(message)s"
        }
    },

    "handlers": {
        'console':{
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        }

    },

    "root": {
        "level": "INFO",
        "handlers": ["console"]
    }
}

logging.config.dictConfig(loggingConfig)
