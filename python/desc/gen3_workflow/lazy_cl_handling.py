"""
Functions to set environment variables and file paths in command
lines at the plugin level.  Environment variables are left in the form
${<env var>} so that the shell can evaluated them at execution time.
File paths are set using the bare GenericWorkflowFile.src_uri values,
so that everything is assumed to be run on a shared file system.
"""
import re


__all__ = ['fix_env_var_syntax', 'get_input_file_paths', 'insert_file_paths']


def fix_env_var_syntax(oldstr):
    """Replace '<ENV: env_var>' with '${env_var}' througout the input string."""
    newstr = oldstr
    for key in re.findall(r"<ENV:([^>]+)>", oldstr):
        newstr = newstr.replace(rf"<ENV:{key}>", "${%s}" % key)
    return newstr


def get_input_file_paths(generic_workflow, job_name):
    """Return a dictionary of file paths, keyed by input name."""
    file_paths = dict()
    for item in generic_workflow.get_job_inputs(job_name):
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
