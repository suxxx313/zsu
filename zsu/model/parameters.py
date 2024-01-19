import os


class Parameters(object):
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    resource_dir = os.path.join(root_dir, "resources")
    model_artifact_dir = os.path.join(resource_dir, "model_artifacts")

    def __init__(self):
        self.modelversion = None

    @property
    def model_dir(self) -> str:
        return os.path.join(self.model_artifact_dir, f"{self.modelversion}")
