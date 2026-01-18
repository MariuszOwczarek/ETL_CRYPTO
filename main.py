from ETL_Pipeline import ETLPipeline
import yaml


def main():
    def read_configurator():
        with open('config/config.yaml', 'r') as f:
            return yaml.safe_load(f)

    config = read_configurator()
    pipeline = ETLPipeline(config)

    pipeline.ensure_dirs()
    raw_obj = pipeline.fetch()
    processed_obj = pipeline.clean(raw_obj)
    pipeline.spark_transform(processed_obj)


if __name__ == "__main__":
    main()
