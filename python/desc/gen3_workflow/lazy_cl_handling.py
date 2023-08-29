"""
Functions to set environment variables and file paths in command
lines at the plugin level.  Environment variables are left in the form
${<env var>} so that the shell can evaluate them at execution time.
File paths are set using the bare GenericWorkflowFile.src_uri values,
so that everything is assumed to be run on a shared file system.
"""
import os
import glob
import shutil
import re


__all__ = ['fix_env_var_syntax', 'get_input_file_paths', 'insert_file_paths',
           'resolve_env_vars']


def resolve_env_vars(oldstr):
    """
    Replace '<ENV: env_var>' with `os.environ[env_var]` througout the
    input string.
    """
    newstr = oldstr
    for key in re.findall(r"<ENV:([^>]+)>", oldstr):
        newstr = newstr.replace(rf"<ENV:{key}>", "%s" % os.environ[key])
    return newstr


def fix_env_var_syntax(oldstr):
    """
    Replace '<ENV: env_var>' with '${env_var}' throughout the input string.
    """
    newstr = oldstr
    for key in re.findall(r"<ENV:([^>]+)>", oldstr):
        newstr = newstr.replace(rf"<ENV:{key}>", "${%s}" % key)
    return newstr


def exec_butler_tmp_dir(exec_butler_dir, job_name, tmp_dirname):
    """Construct the job-specific path for the non-shared copy of the
    execution butler repo."""
    return os.path.join(os.path.dirname(exec_butler_dir), tmp_dirname,
                        job_name)


def get_input_file_paths(generic_workflow, job_name, tmp_dirname='tmp_repos'):
    """Return a dictionary of file paths, keyed by input name."""
    file_paths = dict()
    for item in generic_workflow.get_job_inputs(job_name):
        if (item.name == 'butlerConfig' and not item.job_shared and
            job_name != 'pipetaskInit'):  # pipetaskInit needs special handling
            exec_butler_dir = os.path.dirname(item.src_uri) \
                if item.src_uri.endswith('butler.yaml') else item.src_uri
            file_paths[item.name] \
                = exec_butler_tmp_dir(exec_butler_dir, job_name, tmp_dirname)
        else:
            file_paths[item.name] = resolve_env_vars(item.src_uri)
    return file_paths


def insert_file_paths(command, file_paths, start_delim='<FILE:', end_delim='>'):
    """
    Insert file paths into the command line at locations indicated by
    the starting and ending delimiters.
    """
    tokens = command.split()
    final_tokens = []
    for token in tokens:
        if token.startswith(start_delim) and token.endswith(end_delim):
            key = token[len(start_delim):-len(end_delim)]
            final_tokens.append(file_paths[key])
        else:
            final_tokens.append(token)
    return ' '.join(final_tokens)
