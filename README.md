# BigPanDAmon

BigPanDAmon is a web-based monitoring application designed for the PanDA (Production and Distributed Analysis) Workload Management System. 
It provides real-time insights into tasks, jobs, and overall system health within a high-energy physics computing environment.

## Features

*   **Comprehensive Monitoring:** Visualize and track PanDA tasks, jobs, and workflows.
*   **Modular Architecture:** Composed of core views and specialized modules including:
    *   `art`: ATLAS Release Tester monitoring.
    *   `buildmonitor`: ATLAS Athena Nightlies CI/CD and build monitoring.
    *   `datacarousel`: ATLAS Data Carousel staging and tape-to-disk data transfer monitoring.
*   **Data Integration:** Connects with [Rucio](https://github.com/rucio/rucio) (Scientific Data Management), [iDDS](https://github.com/HSF/iDDS) (intelligent Data Delivery Service), OpenSearch, and various databases (supporting Oracle and PostgreSQL).

## Tech Stack

*   **Framework:** Django (Python)
*   **Databases:** PostgreSQL, Oracle
*   **Search/Analytics:** OpenSearch
*   **Asynchronous:** Django Channels (with Daphne)
*   **Infrastructure:** Docker, AlmaLinux

## Documentation

Detailed documentation can be found [here](https://panda-wms.readthedocs.io/en/latest/client/monitoring.html).