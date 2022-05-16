import parsl

__all__ = ['bash_app_3G', 'bash_app_6G', 'bash_app_8G', 'bash_app_16G', 'bash_app_24G', 'bash_app_60G', 'bash_app_180G',
           'local_bash_app']

ignore_for_cache = ['stdout', 'stderr']
#, 'wrap', 'parsl_resource_specification']

bash_app_3G = parsl.bash_app(executors=['batch-3G'], cache=True,
                                ignore_for_cache=ignore_for_cache)

bash_app_6G = parsl.bash_app(executors=['batch-6G'], cache=True,
                                ignore_for_cache=ignore_for_cache)

bash_app_8G = parsl.bash_app(executors=['batch-8G'], cache=True,
                                ignore_for_cache=ignore_for_cache)

bash_app_16G = parsl.bash_app(executors=['batch-16G'], cache=True,
                                ignore_for_cache=ignore_for_cache)

bash_app_24G = parsl.bash_app(executors=['batch-24G'], cache=True,
                                ignore_for_cache=ignore_for_cache)

bash_app_60G = parsl.bash_app(executors=['batch-60G'], cache=True,
                                ignore_for_cache=ignore_for_cache)

bash_app_180G = parsl.bash_app(executors=['batch-180G'], cache=True,
                                ignore_for_cache=ignore_for_cache)

local_bash_app = parsl.bash_app(executors=['submit-node'], cache=True,
                                ignore_for_cache=ignore_for_cache)
