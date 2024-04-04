import math

import pandas as pd
import bokeh.transform
import bokeh.models
import bokeh.plotting
import bokeh.layouts
import bokeh.embed
import bokeh.io


def grouped_bar_plot(data_daily):
    """Create Bar-Plots grouped by years"""

    data_yearly = data_daily.iloc[:, :-1].groupby(data_daily.index.year).mean()
    data_yearly = data_yearly.reset_index().astype({"date": str}).set_index("date")

    source = bokeh.models.ColumnDataSource(data=data_yearly.reset_index())

    color = "#BBBBBB #F0E442 #D55E00 #009E73 #0072B2 #56B4E9".split()

    bar_plot = bokeh.plotting.figure(
        x_range=data_yearly.index.to_list(),
        y_range=(0, 3500),
        title="Average Energy Production by Year and Source",
        height=350,
        toolbar_location=None,
        tools="",
        y_axis_label="Energy Production [MW]",
    )

    for i, col_name in enumerate(data_yearly.columns):

        # Position of the Bars
        x_offset = 0.7 * ((i + 0.5) / (data_yearly.shape[1]) - 0.5)

        bar_plot.vbar(
            x=bokeh.transform.dodge(
                field_name="date", value=x_offset, range=bar_plot.x_range
            ),
            top=col_name,
            source=source,
            width=0.1,
            color=color[i],
            legend_label=col_name,
        )

    bar_plot.x_range.range_padding = 0.1
    bar_plot.xgrid.grid_line_color = None
    bar_plot.add_layout(bar_plot.legend[0], "right")  # Legend outside of the plot

    fig = bokeh.layouts.column(bar_plot)

    print("Bar-Plot ready to plot")

    return fig


def yearly_pie_plot(data_daily):
    """Create Pie-Plot for every year"""

    # Create Set of years
    years = set(data_daily.index.year)

    # Create List of years, to sort it
    years_sorted = []
    for year in years:
        years_sorted.append(year)
    years_sorted.sort()

    for year in years_sorted:

        yearly_results = data_daily[data_daily.index.year == year]

        x = round(yearly_results.iloc[:, :-1].sum() / 1000, 2)

        data = (
            pd.Series(x).reset_index(name="value").rename(columns={"index": "country"})
        )
        data["angle"] = data["value"] / data["value"].sum() * 2 * math.pi
        data["color"] = "#BBBBBB #F0E442 #D55E00 #009E73 #0072B2 #56B4E9".split()

        pie_plot = bokeh.plotting.figure(
            height=350,
            title=f"Yearly Production by Energy-Type in GWh in {year}",
            toolbar_location=None,
            tools="hover",
            tooltips="@country: @value",
            x_range=(-0.5, 1.0),
        )

        pie_plot.wedge(
            x=0,
            y=1,
            radius=0.4,
            start_angle=bokeh.transform.cumsum("angle", include_zero=True),
            end_angle=bokeh.transform.cumsum("angle"),
            line_color="white",
            fill_color="color",
            legend_field="country",
            source=data,
        )

        pie_plot.axis.axis_label = None
        pie_plot.axis.visible = False
        pie_plot.grid.grid_line_color = None

        fig = bokeh.layouts.column(pie_plot)

    return fig


def stacked_area_plot(data_hourly, data_daily):
    """Create an interactive Stacked-Area-Plot as a TimeSeries
    Plots can show daily or hourly data."""

    hourly_source = bokeh.models.ColumnDataSource(data=data_hourly)
    daily_source = bokeh.models.ColumnDataSource(data=data_daily)

    column_names = "nuclear solar wind water_reservoir water_river water_pump".split()
    colors = "#D55E00 #F0E442 #BBBBBB #009E73 #0072B2 #56B4E9".split()

    middle = len(data_hourly) // 2
    selection_range = 500

    # Range-Plot
    range_plot = bokeh.plotting.figure(
        height=300,
        width=800,
        x_axis_type="datetime",
        x_axis_location="above",
        x_range=(
            data_hourly.index[middle - selection_range],
            data_hourly.index[middle + selection_range],
        ),
        y_axis_label="Energy Production [MW]",
    )

    # Range-Plot on daily base
    daily_plot = range_plot.varea_stack(
        x="date",
        stackers=column_names,
        source=daily_source,
        color=colors,
        legend_label=column_names,
    )

    # Range-Plot on hourly base
    hourly_plot = range_plot.varea_stack(
        x="datetime",
        stackers=column_names,
        source=hourly_source,
        color=colors,
        legend_label=column_names,
        visible=False,
    )

    range_plot.legend.location = "top_left"

    # Plot-Selector
    # to change visibility of daily_plot and hourly_plot
    plot_selector = bokeh.models.RadioButtonGroup(
        labels=["Stündlich", "Täglich"], active=1
    )
    plot_selector.js_on_event(
        "button_click",
        bokeh.models.CustomJS(
            args=dict(
                btn=plot_selector,
                hplot=hourly_plot,
                dplot=daily_plot,
            ),
            code="""
        console.log('radio_button_group: active=' + btn.active, this.toString())    
        let plots=[hplot, dplot]
        plots.forEach(p => p.forEach(q => {q.visible=false}))
        plots[btn.active].forEach(q => {q.visible=true})
    """,
        ),
    )

    # Range-Tool
    # for the Overview-Plot
    range_tool = bokeh.models.RangeTool(x_range=range_plot.x_range)
    range_tool.overlay.fill_color = "navy"
    range_tool.overlay.fill_alpha = 0.2

    # Overview-Plot
    overview_plot = bokeh.plotting.figure(
        title="Drag the middle and edges of the selection box to change the range above",
        height=130,
        width=800,
        y_range=range_plot.y_range,
        x_axis_type="datetime",
        y_axis_type=None,
        toolbar_location=None,
    )

    overview_plot.varea_stack(
        x="date",
        stackers=column_names,
        source=daily_source,
        color=colors,
    )

    overview_plot.add_tools(range_tool)

    # Create Figure
    fig = bokeh.layouts.column(plot_selector, range_plot, overview_plot)

    return fig
