# Apache Calcite RelBuilder playground

## How to run

- `docker build -t foodmart .` to build a container with PostgreSQL database and Foodmart dataset data upload scripts.
- `docker run --rm -ti -p5432:5432 foodmart` to run a disposable instance of that container that loads Foodmart data when started.
- wait until the data is loaded (the container outputs `PostgreSQL init process complete; ready for start up.`).
- `sbt test` runs SQL vs RelBuilder test.
- `sbr run` runs just RelBuilder-based queries and outputs results.