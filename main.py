from ETL_Pipeline import ETLPipeline
import yaml


def main():
    def read_configurator():
        with open('config/config.yaml', 'r') as f:
            return yaml.safe_load(f)

    config = read_configurator()
    pipeline = ETLPipeline(config)

    pipeline.ensure_dirs()
    filename = pipeline.fetch()
    pipeline.clean(filename)
    pipeline.spark_transform()
    pipeline.preview_parquet(5)


if __name__ == "__main__":
    main()
