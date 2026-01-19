from ETL_Pipeline import ETLPipeline
import yaml


def main():
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    pipeline = ETLPipeline(config)
    pipeline.ensure_dirs()
    raw_obj = pipeline.fetch()
    processed_obj = pipeline.clean(raw_obj)
    pipeline.spark_transform(processed_obj)


if __name__ == "__main__":
    main()
