import os
import sys
import importlib.util
import logging

logger = logging.getLogger(__name__)


class ConfigsHelper:
    """The ConfigsHelper class is used to load parameters from configuration files."""

    def __init__(self, configs_folder: str = "configurations"):
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

    def find_and_load_configs_folder(self, base_dir, configs_folder) -> bool:
        for root, dirs, files in os.walk(base_dir):
            logger.debug(f"Checking {root} for {configs_folder}")
            if configs_folder in dirs:
                # Set the configs directory
                self.configs_dir = os.path.join(root, configs_folder)
                logger.info(f"Configs directory found at: {self.configs_dir}")
                return True
        return False

    def load_config(self, config_name: str):
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
        config_path = os.path.join(self.configs_dir, f"{config_name}.py")
        spec = importlib.util.spec_from_file_location(config_name, config_path)
        module = importlib.util.module_from_spec(spec)

        try:
            spec.loader.exec_module(module)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file {config_path} does not exist")
        except Exception as e:
            raise ImportError(f"Error loading configuration file {config_path}: {e}")

        # If the configuration file does not have a parameters attribute, throw an error
        if not hasattr(module, 'parameters'):
            raise AttributeError(f"Configuration file {config_name} does not have a parameters attribute")

        # Get the parameters from the configuration file
        parameters = module.parameters

        logger.info(f"Loaded configuration file {config_name}")
        return parameters
