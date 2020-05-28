{% macro stitch_bing_keyword_performance() %}
    {{ adapter_macro('bing_ads.stitch_bing_keyword_performance') }}
{% endmacro %}

{% macro default__stitch_bing_keyword_performance() %}
    WITH source AS (
        SELECT
            *
        FROM
            {{ var('bing_keyword_performance_report_table') }}
    ),
    renamed AS (
        SELECT
            "__SDC_PRIMARY_KEY" AS keyword_performance_report_id,
            CONVERT_TIMEZONE(
                'UTC',
                timeperiod
            ) :: timestamp_ntz :: DATE AS campaign_date,
            accountid AS account_id,
            adgroupid AS ad_group_id,
            adgroupname AS ad_group_name,
            adid AS ad_id,
            campaignid AS campaign_id,
            campaignname AS campaign_name,
            keywordid AS keyword_id,
            campaignstatus AS campaign_status,
            REPLACE(COALESCE(destinationurl, '') || COALESCE(finalurl, ''), '%20', ' ') AS url,
            clicks,
            impressions,
            spend,
            RANK() over (
                PARTITION BY timeperiod :: DATE
                ORDER BY
                    _sdc_report_datetime DESC
            ) AS RANK
        FROM
            source
    ),
    parsed AS (
        SELECT
            keyword_performance_report_id,
            campaign_date,
            keyword_id,
            account_id,
            ad_group_id,
            ad_group_name,
            ad_id,
            campaign_id,
            campaign_name,
            campaign_status,
            url,
            {{ dbt_utils.get_url_host('url') }} AS url_host,
            '/' || {{ dbt_utils.get_url_path('url') }} AS url_path,
            {{ dbt_utils.get_url_parameter(
                'url',
                'utm_source'
            ) }} AS utm_source,
            {{ dbt_utils.get_url_parameter(
                'url',
                'utm_medium'
            ) }} AS utm_medium,
            {{ dbt_utils.get_url_parameter(
                'url',
                'utm_campaign'
            ) }} AS utm_campaign,
            {{ dbt_utils.get_url_parameter(
                'url',
                'utm_content'
            ) }} AS utm_content,
            {{ dbt_utils.get_url_parameter(
                'url',
                'utm_term'
            ) }} AS utm_term,
            clicks,
            impressions,
            spend
        FROM
            renamed
        WHERE
            RANK = 1
    )
SELECT
    *
FROM
    parsed
{% endmacro %}

{% macro bigquery__stitch_bing_keyword_performance() %}
    WITH source AS (
        SELECT
            *
        FROM
            {{ var('bing_keyword_performance_report_table') }}
    ),
    renamed AS (
        SELECT
            "__SDC_PRIMARY_KEY" AS keyword_performance_report_id,
            DATE(
                timeperiod,
                'UTC'
            ) AS campaign_date,
            accountid AS account_id,
            adgroupid AS ad_group_id,
            adgroupname AS ad_group_name,
            adid AS ad_id,
            campaignid AS campaign_id,
            campaignname AS campaign_name,
            keywordid AS keyword_id,
            campaignstatus AS campaign_status,
            REPLACE(COALESCE(finalurl, ''), '%20', ' ') AS url,
            clicks,
            impressions,
            spend,
            RANK() over (
                PARTITION BY timeperiod
                ORDER BY
                    _sdc_report_datetime DESC
            ) AS RANK
        FROM
            source
    ),
    parsed AS (
        SELECT
            keyword_performance_report_id,
            campaign_date,
            keyword_id,
            account_id,
            ad_group_id,
            ad_group_name,
            ad_id,
            campaign_id,
            campaign_name,
            campaign_status,
            url,
            {{ dbt_utils.get_url_host('url') }} AS url_host,
            CONCAT('/', {{ dbt_utils.get_url_path('url') }}) AS url_path,
            {{ dbt_utils.get_url_parameter(
                'url',
                'utm_source'
            ) }} AS utm_source,
            {{ dbt_utils.get_url_parameter(
                'url',
                'utm_medium'
            ) }} AS utm_medium,
            {{ dbt_utils.get_url_parameter(
                'url',
                'utm_campaign'
            ) }} AS utm_campaign,
            {{ dbt_utils.get_url_parameter(
                'url',
                'utm_content'
            ) }} AS utm_content,
            {{ dbt_utils.get_url_parameter(
                'url',
                'utm_term'
            ) }} AS utm_term,
            clicks,
            impressions,
            spend
        FROM
            renamed
        WHERE
            RANK = 1
    )
SELECT
    *
FROM
    parsed
{% endmacro %}
