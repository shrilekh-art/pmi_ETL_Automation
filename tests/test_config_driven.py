from framework.validation_runner import run_validations


def test_config_framework():
    run_validations("configs/pipeline_config.json")