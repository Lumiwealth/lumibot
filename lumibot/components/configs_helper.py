import importlib.util
import os
import sys
from collections.abc import Mapping
from typing import Any, cast

from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)


class ConfigsHelper:
    """The ConfigsHelper class is used to load parameters from configuration files."""

    configs_dir: str | None

    def __init__(self, configs_folder: str = "configurations") -> None:
        """
        Parameters
        ----------
        configs_folder : str
            The folder where the configs are stored. Default is "configurations".
        """
        self.configs_dir = None

        # Get the current directory of where the script is running (the original script that is calling this class)
        current_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
        found_and_loaded_configs_folder = self.find_and_load_configs_folder(current_dir, configs_folder)

        if not found_and_loaded_configs_folder:
            # Get the root directory of the project
            cwd_dir = os.getcwd()
            logger.debug(f"cwd_dir: {cwd_dir}")
            found_and_loaded_configs_folder = self.find_and_load_configs_folder(cwd_dir, configs_folder)

        # If no configs folder was found, throw an error
        if not found_and_loaded_configs_folder:
            raise FileNotFoundError(f"Configs folder {configs_folder} not found")

    def find_and_load_configs_folder(self, base_dir: str, configs_folder: str) -> bool:
        for root, dirs, _files in os.walk(base_dir):
            logger.debug(f"Checking {root} for {configs_folder}")
            if configs_folder in dirs:
                # Set the configs directory
                self.configs_dir = os.path.join(root, configs_folder)
                logger.info(f"Configs directory found at: {self.configs_dir}")
                return True
        return False

    def load_config(self, config_name: str) -> dict[str, Any]:
        """
        Load the parameters from a configuration file.

        Parameters
        ----------
        config_name : str
            The name of the configuration file.

        Returns
        -------
        dict
            The parameters from the configuration file
        """

        # Get the configuration file
        configs_dir = self.configs_dir
        if configs_dir is None:
            raise FileNotFoundError("Configs directory has not been initialized")

        config_path = os.path.join(configs_dir, f"{config_name}.py")
        spec = importlib.util.spec_from_file_location(config_name, config_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load configuration file spec for {config_path}")
        module = importlib.util.module_from_spec(spec)

        try:
            spec.loader.exec_module(module)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file {config_path} does not exist") from None
        except Exception as e:
            raise ImportError(f"Error loading configuration file {config_path}: {e}") from e

        # If the configuration file does not have a parameters attribute, throw an error
        if not hasattr(module, "parameters"):
            raise AttributeError(f"Configuration file {config_name} does not have a parameters attribute")

        # Get the parameters from the configuration file
        parameters = module.parameters
        if not isinstance(parameters, Mapping):
            raise TypeError(f"Configuration file {config_name} parameters must be a mapping")

        logger.info(f"Loaded configuration file {config_name}")
        return dict(cast(Mapping[str, Any], parameters))
