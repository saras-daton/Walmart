{% if var('WalmartItemReport') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}


{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}



{% set table_name_query %}
{{set_table_name('%walmart%itemreport')}}    
{% endset %}


{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
{% set results_list = [] %}
{% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours')%}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

    SELECT * {{exclude()}}(row_num)
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        ReportDate,
        PARTNER_ID,
        SKU,
        PRODUCT_NAME,
        PRODUCT_CATEGORY,
        PRICE,
        CURRENCY,
        BUY_BOX_ITEM_PRICE,
        BUY_BOX_SHIPPING_PRICE,
        PUBLISH_STATUS,
        STATUS_CHANGE_REASON,
        LIFECYCLE_STATUS,
        INVENTORY_COUNT,
        SHIP_METHODS,
        WPID,
        ITEM_ID,
        GTIN,
        UPC,
        PRIMARY_IMAGE_URL,
        SHELF_NAME,
        PRIMARY_CAT_PATH,
        OFFER_START_DATE,
        OFFER_END_DATE,
        ITEM_CREATION_DATE,
        ITEM_LAST_UPDATED,
        ITEM_PAGE_URL,
        REVIEWS_COUNT,
        AVERAGE_RATING,
        SEARCHABLE__,
        COMPETITOR_URL,
        COMPETITOR_PRICE,
        COMPETITOR_SHIP_PRICE,
        COMPETITOR_LAST_DATE_FETCHED,
        BRAND as Item_Brand,
        PRODUCT_TAX_CODE,
        MSRP,
        SHIPPING_WEIGHT,
        SHIPPING_WEIGHT_UNIT,
        FULFILLMENT_LAG_TIME,
        FULFILLMENT_TYPE,
        WFS_SALES_RESTRICTION,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then a.CURRENCY else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            a.CURRENCY as exchange_currency_code, 
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        ROW_NUMBER() OVER (PARTITION BY ITEM_ID, SKU order by a.{{daton_batch_runtime()}} desc) row_num
        FROM  {{i}} a
            {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(a.ReportDate) = c.date and a.CURRENCY = c.to_currency_code
            {% endif %}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
        )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}



