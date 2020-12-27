import parsl

__all__ = ['small_bash_app', 'medium_bash_app', 'large_bash_app',
           'local_bash_app']

ignore_for_cache = ['stdout', 'stderr']
#, 'wrap', 'parsl_resource_specification']

small_bash_app = parsl.bash_app(executors=['cori-small'], cache=True,
                                ignore_for_cache=ignore_for_cache)

medium_bash_app = parsl.bash_app(executors=['cori-medium'], cache=True,
                                ignore_for_cache=ignore_for_cache)

large_bash_app = parsl.bash_app(executors=['cori-large'], cache=True,
                                ignore_for_cache=ignore_for_cache)

local_bash_app = parsl.bash_app(executors=['submit-node'], cache=True,
                                ignore_for_cache=ignore_for_cache)
