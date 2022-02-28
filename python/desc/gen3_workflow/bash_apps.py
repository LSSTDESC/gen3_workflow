import parsl

__all__ = ['bash_app_3G', 'bash_app_9G', 'bash_app_18G', 'bash_app_48G', 'bash_app_120G',
           'local_bash_app']

ignore_for_cache = ['stdout', 'stderr']
#, 'wrap', 'parsl_resource_specification']

bash_app_3G = parsl.bash_app(executors=['batch-3G'], cache=True,
                                ignore_for_cache=ignore_for_cache)

bash_app_9G = parsl.bash_app(executors=['batch-9G'], cache=True,
                                ignore_for_cache=ignore_for_cache)

bash_app_18G = parsl.bash_app(executors=['batch-18G'], cache=True,
                                ignore_for_cache=ignore_for_cache)

bash_app_48G = parsl.bash_app(executors=['batch-48G'], cache=True,
                                ignore_for_cache=ignore_for_cache)

bash_app_120G = parsl.bash_app(executors=['batch-120G'], cache=True,
                                ignore_for_cache=ignore_for_cache)

local_bash_app = parsl.bash_app(executors=['submit-node'], cache=True,
                                ignore_for_cache=ignore_for_cache)
