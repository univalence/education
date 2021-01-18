# Distributed computation - Lab 01

In this lab, we will discover Apache Spark through a notebook. A
**notebook** is an application that helps you to write documents like
reports, articles, books... integrating executable code. So a
**notebook** is associated to execution environments, named _kernels_.

In this lab, we will learn to manipulate _Apache Spark_ with the Scala
language, with the help of the notebook _Jupyter_. Jupyter is one of
the most widely used notebook, especially among the data scientist
community. It comes as a Web application.

Apart from Docker, you will not have anything to install.

## Docker

To run this lab, you have to install Docker first. You can get Docker
through this link: https://www.docker.com/get-started (select
**Download** button in the _Docker Desktop_ section).

## Launch the notebook

To access the notebook you will have to launch a specific container
`univalence/tout-spark-notebook`.

Under Unix/Linux/OSX:

```sh
$ docker run -it --rm -p 8888:8888 -p 4040:4040 --cpus=2 --memory=2048M -v "$PWD":/home/jovyan/work univalence/tout-spark-notebook
```

Under Windows:

```cmd
% docker run -it --rm -p 8888:8888 -p 4040:4040 --cpus=2 --memory=2048M -v "%CD%":/home/jovyan/work univalence/tout-spark-notebook
```

Note: `8888` is the listening port of Jupyter and `4040` is the
listening port for Spark UI.

Once launched, you will see some logs in your console and a token.
**Copy this token**.

Open your browser on the URL http://localhost:8888/.
**Paste the token in the appearing Jupyter page**.

Once validated, you will a directory structure.
**Click on the `work` directory and find the a notebook**.
Notebooks have the extension `*.ipynb`.
