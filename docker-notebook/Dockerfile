
FROM jupyter/all-spark-notebook

LABEL maintainer="Univalence <hello@univalence.io>"

RUN pip install --quiet --upgrade toree && \
    jupyter toree install --sys-prefix --spark_home=/usr/local/spark/ && \
    rm -rf "/home/${NB_USER}/.local" && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

