import logging
import os
import re

from databricks.sdk import WorkspaceClient

logging.basicConfig(format='[%(levelname)s] %(asctime)s [databricks] %(funcName)s %(message)s', level=logging.INFO)
logger = logging.getLogger()


def get_workspace_client(
        host: str,
        client_id: str,
        client_secret):
    return WorkspaceClient(
        host=host,
        client_id=client_id,
        client_secret=client_secret
    )


def if_not_exists_create_git_credential(w: WorkspaceClient, git_username: str, git_token: str):
    logger_event = 'create'
    try:
        _credential_id = list(w.git_credentials.list())[0].credential_id
        logger_event = 'get'
    except IndexError:
        git_credential = w.git_credentials.create(git_provider='gitHub', git_username=git_username, personal_access_token=git_token)
        _credential_id = git_credential.credential_id
    logger.info(f'[git_credential_id={_credential_id}] event={logger_event}')
    return _credential_id


def get_repository_name(git_url):
    pattern = re.compile("([^/]+)\\.git$")
    matcher = pattern.search(git_url)
    if matcher:
        return matcher.group(1)
    else:
        raise RuntimeError(f'Could not identify some repository name on URL = {git_url}')


def mkdirs(w_: WorkspaceClient, path: str):
    w_.workspace.mkdirs(path=path)


def get_or_create_repo_id(w: WorkspaceClient, repository_url: str, username: str):
    user_repository_path = f'/Repos/{username}'
    mkdirs(w_=w, path=user_repository_path)
    repository_target_path_ = f'{user_repository_path}/{get_repository_name(repository_url)}.git'
    try:
        _repo_id = list(w.repos.list(path_prefix=repository_target_path_))[0].id
    except IndexError:
        repo = w.repos.create(provider='gitHub', url=repository_url, path=repository_target_path_)
        _repo_id = repo.id
        logger.info(f'[repo_id={_repo_id}] git clone {repository_url}')

    if _repo_id is None:
        raise RuntimeError(
            f'Could not create the repository!!! url=[{repository_url}], user_repository_path=[{user_repository_path}], repository_target_path_=[{repository_target_path_}]')
    logger.info(f'[repo_id={_repo_id}] user_repository_path={user_repository_path}, repository_target_path_={repository_target_path_}')
    return _repo_id


def git_checkout_and_pull(w: WorkspaceClient, repo_id: int, branch: str, tag: str):
    if branch is None and tag is None:
        raise ValueError(f'A [branch] or [tag] is required to update the repository')
    logger.info(f'[repo_id={repo_id}, branch={branch}, tag={tag}] git checkout and pull')
    w.repos.update(repo_id=repo_id, branch=branch, tag=tag)


def git_repo_delete(w: WorkspaceClient, repo_id: int):
    logger.info(f'[repo_id={repo_id}] event=delete')
    w.repos.delete(repo_id=repo_id)


def get_env(env):
    env_value = os.environ.get(env)
    if env_value is None:
        raise ValueError(f'Environment variable [{env}] had not found!!!')
    return env_value


def main():
    logger.info(f'Starting')

    _w = get_workspace_client(
        host=get_env('DATALAKE_DATABRICKS_WORKSPACE_URL'),
        client_id=get_env('DATALAKE_DATABRICKS_CLIENT_ID'),
        client_secret=get_env('DATALAKE_DATABRICKS_CLIENT_SECRET'))

    if_not_exists_create_git_credential(
        w=_w,
        git_username=get_env('DATALAKE_DATABRICKS_GIT_USERNAME'),
        git_token=get_env('DATALAKE_DATABRICKS_GIT_TOKEN'))

    _repo_id = get_or_create_repo_id(
        w=_w,
        repository_url=get_env('DATALAKE_DATABRICKS_REPO_URL'),
        username=get_env('DATALAKE_DATABRICKS_REPO_USER'))

    git_checkout_and_pull(
        w=_w,
        repo_id=_repo_id,
        branch=os.environ.get('DATALAKE_DATABRICKS_GIT_BRANCH_REF'),
        tag=os.environ.get('DATALAKE_DATABRICKS_GIT_TAG_REF'))

    logger.info(f'Finished')


if __name__ == '__main__':
    main()
