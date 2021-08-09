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


__all__ = ['fix_env_var_syntax', 'copy_exec_butler_files',
           'get_input_file_paths', 'insert_file_paths']


def fix_env_var_syntax(oldstr):
    """Replace '<ENV: env_var>' with '${env_var}' througout the input string."""
    newstr = oldstr
    for key in re.findall(r"<ENV:([^>]+)>", oldstr):
        newstr = newstr.replace(rf"<ENV:{key}>", "${%s}" % key)
    return newstr


def copy_exec_butler_files(exec_butler_dir, job_name, tmp_dirname='tmp_repos'):
    """Make a non-shared copy of the butler repo for each job."""
    dest_dir = os.path.join(os.path.dirname(exec_butler_dir), tmp_dirname,
                            job_name)
    os.make_dirs(dest_dir, exist_ok=True)
    for src in glob.glob(os.path.join(exec_butler_dir, '*')):
        dest = os.path.join(dest_dir, os.path.basename(src))
        if not os.path.isfile(dest):
            shutil.copy(src, dest)
    return dest_dir


def get_input_file_paths(generic_workflow, job_name, tmp_dirname='tmp_repos'):
    """Return a dictionary of file paths, keyed by input name."""
    file_paths = dict()
    for item in generic_workflow.get_job_inputs(job_name):
        if (item.name == 'butlerConfig' and not item.job_shared and
            job_name != 'pipetaskInit'):  # pipetaskInit needs special handling
            exec_butler_dir = os.path.dirname(item.src_uri) \
                if item.src_uri.endswith('butler.yaml') else item.src_uri
            dest_dir = copy_exec_butler_files(exec_butler_dir, job_name,
                                              tmp_dirname=tmp_dirname)
            file_paths[item.name] = dest_dir
        else:
            file_paths[item.name] = item.src_uri
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
