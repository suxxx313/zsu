from .parameters import Parameters


class GenericParameters(Parameters):
    def __init__(self):
        super().__init__()

        self.model_name = "GENERIC"


class Generic(object):
    params = GenericParameters()

    @property
    def parameters(self):
        return {
            "artifact_dir": self.params.model_dir,
            "model_name": self.params.modelversion,
            "model_bin": self.params.model_dir + f"/{self.params.modelversion}_Model.bin",
            "var_list": self.params.model_dir + f"/{self.params.modelversion}_VAR_LIST.txt",
        }

    @property
    def spec(self):
        return {
            "ntrees": 999,
            "missing_value": "-99999"
        }

    @property
    def model_var(self):
        return []