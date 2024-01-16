FROM python:3.10-bookworm

RUN curl -sSL https://install.python-poetry.org | python3 - --version 1.4.1
# Add Poetry bin directory to PATH environment variable
ENV PATH="${PATH}:/root/.local/bin"

COPY scripts /scripts
COPY projects/data-pipeline /projects/data-pipeline

WORKDIR /projects/data-pipeline/

RUN poetry env use 3.10.4
RUN poetry install --no-root

CMD ["make", "run"]
