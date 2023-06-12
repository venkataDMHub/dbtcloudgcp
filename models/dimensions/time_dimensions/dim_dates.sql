{% set market_closed_days = {'new_years_day':('2020-01-01','2021-01-01','2023-01-02','2024-01-01','2025-01-01'), 
                            'mlk_day':('2020-01-20','2021-01-18','2022-01-17','2023-01-16','2024-01-15','2025-01-20'), 
                            'washingtons_birthday':('2020-02-17','2021-02-15','2022-02-21','2023-02-20','2024-02-19','2025-02-17'), 
                            'good_friday':('2020-04-10','2021-04-02','2022-04-15','2023-04-07','2024-03-29','2025-04-18'),
                            'memorial_day':('2020-05-25','2021-05-31','2022-05-30','2023-05-29','2024-05-27','2025-05-26'),
                            'juneteenth':('2022-06-20','2023-06-19','2024-06-19','2025-06-19'),
                            'independence_day':('2020-07-03','2021-07-05','2022-07-04','2023-07-04','2024-07-04','2025-07-04'),
                            'labor_day':('2020-09-07','2021-09-06','2022-09-05','2023-09-04','2024-09-02','2025-09-01'),
                            'thanksgiving_day':('2020-11-26','2021-11-25','2022-11-24','2023-11-23','2024-11-28','2025-11-27'),
                            'christmas_day': ('2020-12-25','2021-12-24','2022-12-26','2023-12-25','2024-12-25','2025-12-25') 
                            }
  %}  

with date_spine as (
    {{ dbt_utils.date_spine(
            datepart="day",
            start_date="to_date('2017-01-01')",
            end_date="to_date('2026-01-01')"
        )
    }}
)
select cast(d.DATE_DAY as date) as DATE,
       dayofweek(DATE)          as DAY_OF_WEEK,
       decode(DAY_OF_WEEK,
              1, 'Monday',
              2, 'Tuesday',
              3, 'Wednesday',
              4, 'Thursday',
              5, 'Friday',
              6, 'Saturday',
              0, 'Sunday'
           )                    as WEEK_DAY_NAME,
       left(WEEK_DAY_NAME, 3)   as WEEK_DAY_NAME_ABR,
       date_trunc(week, DATE)   as FIRST_DAY_OF_WEEK,
       last_day(DATE, week)     as LAST_DAY_OF_WEEK,
       dayofmonth(DATE)         as DAY_OF_MONTH,
       decode(MONTH(DATE),
              1, 'January',
              2, 'February',
              3, 'March',
              4, 'April',
              5, 'May',
              6, 'June',
              7, 'July',
              8, 'August',
              9, 'September',
              10, 'October',
              11, 'November',
              12, 'December'
           )                    as MONTH_NAME,
       left(MONTH_NAME, 3)      as MONTH_NAME_ABR,
       date_trunc(month, DATE)  as FIRST_DAY_OF_MONTH,
       last_day(DATE, month)    as LAST_DAY_OF_MONTH,
       dayofyear(DATE)          as DAY_OF_YEAR,
       weekofyear(DATE)         as WEEK_OF_YEAR,
       month(DATE)              as MONTH_OF_YEAR,
       date_trunc(quarter, DATE) as FIRST_DAY_OF_QUARTER,
       date_trunc(year, DATE)   as FIRST_DAY_OF_YEAR,
       last_day(DATE, year)     as LAST_DAY_OF_YEAR,
       case
        {% for key, value in market_closed_days.items() %}
            when DATE_DAY in {{value}} then '{{key}}' 
                {% endfor %}
            else null
            end as holiday_name_usa,
        IFF(holiday_name_usa is not null, TRUE, FALSE) as is_market_closed_date_usa
from date_spine d
