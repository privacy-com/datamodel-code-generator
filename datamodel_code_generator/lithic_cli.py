import argparse
import difflib
import logging
import os
import shlex
import shutil
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
    output_module_path: str


class SourceConfig(BaseModelConfig):
    input: str
    repo: Optional[str] = None
    branch: Optional[str] = None
    args: Optional[str] = None
    output_module_path: Optional[str] = None


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


def list_files(directory: str):
    for root, _, files in os.walk(directory):
        for file in files:
            yield os.path.join(root, file)


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


def run_diff(f1: str, f2: str):
    with open(f1) as f:
        text1 = f.read()

    with open(f2) as f:
        text2 = f.read()

    logger.info(f'Comparing {f1} and {f2}')
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

    outputs = []

    for source in main_config.sources:
        name = source.input.split('/')[-1].split('.')[0]
        try:
            with tempfile.TemporaryDirectory(
                suffix=f'-{name}', delete=False
            ) as tmp_dir:
                cfg = merge_config(main_config.default, source)
                repo_path = run_clone_repo(cfg.repo, cfg.branch)
                final_path = f'{tmp_dir}/{cfg.output_module_path}'

                cmd_args = cast(List[str], shlex.split(cfg.args))  # type: ignore
                cmd_args += ['--input', f'{repo_path}/{cfg.input}']
                cmd_args += ['--output', final_path]

                if '--custom-file-header' not in cmd_args:
                    cmd_args += ['--custom-file-header', '# lithic-schemagen']

                create_module(final_path)
                logger.info(f'Generating schema to {final_path}')

                generate(cmd_args)
                run_ruff(tmp_dir)
                outputs.append((source, final_path))

        except Exception as e:
            logger.error(f'Error processing {source.input}: {e}')
            failed = True
            continue

    if args.generate:
        module_paths = set()
        for source, output_dir in outputs:
            cfg = merge_config(main_config.default, source)
            module_paths.add(cfg.output_module_path)

        for path in module_paths:
            logger.info(f'Deleting ./{path}')
            shutil.rmtree(path, ignore_errors=True)

        for source, output_dir in outputs:
            cfg = merge_config(main_config.default, source)
            logger.info(
                f'Copying generated schema from {output_dir} to {cfg.output_module_path}'
            )
            shutil.copytree(output_dir, cfg.output_module_path, dirs_exist_ok=True)

    else:
        existing_files = set()

        # collect all generated files to check for extra files
        for source, output_dir in outputs:
            cfg = merge_config(main_config.default, source)
            existing_files.update(list_files(cfg.output_module_path))

        for source, output_dir in outputs:
            cfg = merge_config(main_config.default, source)
            for full_path in list_files(output_dir):
                try:
                    fn = os.path.basename(full_path)

                    run_diff(full_path, f'{cfg.output_module_path}/{fn}')
                    try:
                        existing_files.remove(f'{cfg.output_module_path}/{fn}')
                    except:  # noqa: E722
                        pass

                except Exception as e:
                    logger.error(e)
                    failed = True

        if existing_files:
            logger.error(f'Extra files found: {existing_files}')
            failed = True

    if failed:
        quit(1)


if __name__ == '__main__':
    main()
