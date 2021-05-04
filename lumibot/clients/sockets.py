import logging

from .extensions import sockets

namespace = "/client_socket"


@sockets.on("connect", namespace=namespace)
def client_connect():
    logging.info("Client connected")


@sockets.on("disconnect", namespace=namespace)
def client_disconnect():
    logging.info("Client disconnected")
