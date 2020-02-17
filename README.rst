Introduction
------------

The SOS Journaler is a service that writes FIXM data to InfluxDB for querying.
This service reads FIXM data from RabbitMQ through the
`SOS SWIM Consumer`_. These services are managed through Docker Compose.

Configuration
-------------

You must set a few environment variables before running the SOS Journaler so
that it can access SWIM on your behalf.

- ``SWIM_CONNECTION_FACTORY``: The name of the SWIM connection factory
- ``SWIM_QUEUE``: The name of the SWIM message queue to get FIXM data from
- ``SWIM_USERNAME``: The username to log into SWIM with
- ``SWIM_PASSWORD``: The password to log into SWIM with

You can set these environment variables using your shell, a ``.env`` file, or
with a ``docker-compose.override.yml``.

.. code-block:: yaml

   version: "3.7"
   services:
     sos-swim-consumer:
       environment:
         - SWIM_CONNECTION_FACTORY=jondoe.hotmail.com.CF
         - SWIM_QUEUE=jondoe.hotmail.com.FDPS.0a2ce3a4-50f9-4a23-bd32-a7359c028c70.OUT
         - SWIM_USERNAME=jondoe.hotmail.com
         - SWIM_PASSWORD=2bf22e4298934a9273ef15e

Running
-------

After configuring the service, you can start it using Docker Compose. All
Docker images are available through Docker Hub.

.. code-block:: bash

   docker-compose up

.. _SOS SWIM Consumer: https://github.com/sync-or-swim/sos-swim-consumer
