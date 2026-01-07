create or replace api integration udemymc0
    api_provider = git_https_api
    api_allowed_prefixes = ('https://github.com/bcptraining/udemymc/')
    enabled = true
    -- allowed_authentication_secrets = all
    api_user_authentication = (type = snowflake_github_app ) -- enable OAuth support
    comment='CoryP workspace0';
