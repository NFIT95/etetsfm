# Data pipeline

## Overview

This is still very, very much work in progress :D
Thank you for taking the time to go through this.

Current modules go in this direction:
- extractor -> curator
- checker -> curator
- writer -> curator
- checker -> main
- curator -> main
- settings -> main

To run the pipeline and get the .parquet files in projects/data/curated_data:
- Go to projects/data-pipeline
- Run "make run" in your terminal (APP AND DEPENDENCIES MANAGED BY POETRY)

Thank you
Nicola