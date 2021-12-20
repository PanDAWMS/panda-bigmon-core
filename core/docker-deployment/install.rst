#########################################
PanDA monitoring installation with Docker
#########################################
PanDA monitor classic installation is the virtual environment based. However, with the rising popularity of the k8s
deployment model, we added support for Docker. Here we describe the procedure of image preparation and execution with Docker.


*************************************
Building a distributable Docker image
*************************************
The docker build file is located `here <https://github.com/PanDAWMS/panda-bigmon-core/blob/master/core/docker-deployment/Dockerfile>`_.
This file contains commands for pulling the latest PanDA monitor version from the GitHub repository. Therefore this file
could be used alone, and any local uncommitted modifications will not affect the target monitoring image.
The protected information such as a DB connection, passwords, etc. does not come to the image and is supplied in a parameter
file at run-time.
The built image utilizes the test server supplied with the Django. If this image is going to be used in highly concurrent
environment, a more suitable server such as an Nginx or Apache web server should be implicated into the image and
utilized in the particular production context.

There are two files that needs to be supplied at the image building stage: requirements.txt and rucio.cfg. That files
are supplemented in the `docker-deployment <https://github.com/PanDAWMS/panda-bigmon-core/blob/master/core/docker-deployment/>`_
folder. The requirements.txt provides the list of all packages that PanDA monitor needs at run time. That list was created in October 2021
and might be modified to include the upgraded versions of dependencies.

PanDA monitoring uses Rucio for downloading log files and Rucio client needs to have a correspondent configuration file. During
docker image build procedure such a file is copied to the image into a location pre known to Rucio client. Before image build
rucio.cfg should be updated to reflect the actual Rucio servers and accounts to be used. If Rucio is not planned to be used,
this file could be left "as is" but it still needed to the Rucio client at Python packages import step.

After the relevant updates injected into that files, the image build procedure could be executed (:code:`docker build .`).


************************
Running a prebuild image
************************
There are 3 folders with contextual data will be used by the BigPanDA container:
* :code:`logs` for storing logs:
* :code:`settings` with files containing configuration parameters. Templates for creating the settings files are available
in the `GitHub repository <https://github.com/PanDAWMS/panda-bigmon-core/tree/master/core/settings>`_.
* :code:`atlpan` where the updated proxy for Rucio authentication is stored


The deployment configuration also needs an environment variable DEPLOYMENT_BACKEND with information about the DB backend
and the monitoring profile to be used. This variable can be assigned to the following values: POSTGRES, ORACLE_ATLAS,
ORACLE_DOMA.

This is the example of the Docker run command:
.. code-block::

   docker run -it -v $(pwd)/settings:/tmp/settings -v $(pwd)/logs:/var/log/bingpanda -v /data/atlpan/:/data/atlpan/ -e DEPLOYMENT_BACKEND=POSTGRES -p 8000:8000 74c843a0047f

The :code:`74c843a0047f` is the hash of the docker image, :code:`-p 8000:8000` used to map requests to the ports which container web
server is listening.

