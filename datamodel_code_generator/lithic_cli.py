import argparse
import difflib
import logging
import os
import shlex
import subprocess
import tempfile
from functools import cache
from typing import List, Optional, cast

import git
import yaml
from pydantic import BaseModel

from datamodel_code_generator.__main__ import main as generate

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger()


class BaseModelConfig(BaseModel):
    repo: str
    branch: str
    args: str
    output_module: str


class SourceConfig(BaseModelConfig):
    input: str
    repo: Optional[str] = None
    branch: Optional[str] = None
    args: Optional[str] = None
    output_module: Optional[str] = None


class LithicConfig(BaseModel):
    sources: List[SourceConfig]
    default: BaseModelConfig


def create_module(module_path: str):
    init_file = os.path.join(module_path, '__init__.py')

    if os.path.exists(init_file):
        return

    os.makedirs(module_path, exist_ok=True)

    with open(init_file, 'w') as f:
        f.write('')

    logger.info(f'Created module {module_path}')


@cache
def run_clone_repo(repo: str, branch: str):
    with tempfile.TemporaryDirectory(delete=False) as tmp:
        repo = f'git@github.com:{repo}.git'
        logger.info(f'Cloning {repo} on branch {branch} to {tmp}')
        git.Repo.clone_from(repo, tmp, branch=branch)
        return tmp


def run_ruff(file_path: str):
    logger.info(f'Running ruff on {file_path}')
    result = subprocess.run(
        ['ruff', '--fix', file_path], capture_output=True, text=True
    )

    if result.returncode != 0:
        raise ValueError(f'Ruff failed:\n {result.stderr}')

    logger.info('Ruff completed successfully')


def load_config(file_path: str):
    with open(file_path) as file:
        return LithicConfig(**yaml.safe_load(file))


def run_diff(text1: str, text2: str):
    logger.info('Comparing generated schema with current schema')
    diff = difflib.unified_diff(
        text1.splitlines(),
        text2.splitlines(),
        lineterm='',
    )
    diff_list = list(diff)

    if diff_list:
        diff_str = '\n'.join(diff_list)
        raise ValueError(f'Schema comparison failed:\n{diff_str}')

    logger.info('Schema is up to date')


def merge_config(default_config: BaseModelConfig, source_config: SourceConfig):
    merged_config = default_config.dict()
    source_config_dict = source_config.dict(exclude_unset=True)
    for key, value in source_config_dict.items():
        if value is not None:
            merged_config[key] = value
    return SourceConfig(**merged_config)


def main():
    parser = argparse.ArgumentParser(description='lithic-schemagen CLI')
    parser.add_argument('config_path', help='Path to the configuration file')
    parser.add_argument('--generate', action='store_true', help='Generate the schema')

    args = parser.parse_args()
    main_config = load_config(args.config_path)

    failed = False

    for source in main_config.sources:
        name = source.input.split('/')[-1].split('.')[0]

        try:
            with tempfile.NamedTemporaryFile() as tmp:
                cfg = merge_config(main_config.default, source)
                repo_path = run_clone_repo(cfg.repo, cfg.branch)

                cmd_args = cast(List[str], shlex.split(cfg.args))  # type: ignore
                cmd_args += ['--input', f'{repo_path}/{cfg.input}']
                cmd_args += ['--output', tmp.name]
                if '--custom-file-header' not in cmd_args:
                    cmd_args += ['--custom-file-header', '# lithic-schemagen']

                generate(cmd_args)
                run_ruff(tmp.name)

                if args.generate:
                    create_module(cfg.output_module)  # type: ignore
                    with open(f'{cfg.output_module}/{name}.py', 'w') as file:
                        file.write(tmp.read().decode())

                else:
                    with open(f'{cfg.output_module}/{name}.py') as file:
                        current_module = file.read()
                        generated_module = tmp.read().decode()
                        run_diff(current_module, generated_module)

        except Exception as e:
            logger.error(f'Error processing {source.input}: {e}')
            failed = True
            continue

    if failed:
        quit(1)


if __name__ == '__main__':
    main()
