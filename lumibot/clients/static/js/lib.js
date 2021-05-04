// global helpers methods /////////////////////////////////////////
function capitalize(s) {
    if (typeof s !== 'string') return '';
    return s.charAt(0).toUpperCase() + s.slice(1);
}

function parseMoney(amount) {
    return new Intl.NumberFormat(
        'en-US',
        { style: 'currency', currency: 'USD' }
    ).format(amount);
}

function parseTimeString(str) {
    const dt = new Date(str);
    return dt.toISOString().replace("T"," ").slice(0,-5);
}

function checkResponsive(){
    const docWidth = document.documentElement.offsetWidth;

    document.querySelectorAll('*').forEach((el) => {
        if (el.offsetWidth > docWidth) {
            console.log(el);
        }
    });
}

// chart.js helpers /////////////////////////////////////////
function generateChartLineOptions(titleText) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        legend: {
            display: false
        },
        title: {
            display: true,
            titleText,
        },
        scales: {
            x: {
                type: 'time',
                display: true,
                title: {
                    display: true,
                    text: 'Date'
                },
            }
        }
    };
}

function generateChartLineData(labels, data) {
    return {
        labels,
        datasets: [{
            label: 'Portfolio Value',
            backgroundColor: 'rgb(255, 99, 132)',
            borderColor: 'rgb(255, 99, 132)',
            data,
        }]
    };
}

function generateChartLineConfig(titleText, labels, data) {
    return {
        type: 'line',
        data: generateChartLineData(labels, data),
        options: generateChartLineOptions(titleText),
    };
}

function generateDoughnutOptions(labelCallback='') {
    return {
        responsive: true,
        maintainAspectRatio: false,
        indexAxis: 'y',
        plugins: {
            tooltip: {
                callbacks: {
                    label: labelCallback,
                }
            },
        },
    };
}

function generateDoughnutData(labels, data, dataLabel='') {
    const googleChartColors = [
        '#3366CC', '#DC3912', '#FF9900', '#109618', '#990099',
        '#3B3EAC', '#0099C6', '#DD4477', '#66AA00', '#B82E2E',
        '#316395', '#994499', '#22AA99', '#AAAA11', '#6633CC',
        '#E67300', '#8B0707', '#329262', '#5574A6', '#3B3EAC',
    ];
    return {
        labels,
        datasets: [{
            label: dataLabel,
            data,
            backgroundColor: googleChartColors.slice(0, labels.length),
            borderWidth: 1
        }],
    };
}

function generateDoughnutConfig(labels, data, dataLabel='', labelCallback='') {
    return {
        type: 'doughnut',
        data: generateDoughnutData(labels, data, dataLabel),
        options: generateDoughnutOptions(labelCallback),
    }
}

function pushNewChartPoint(chart, label, data) {
    chart.data.labels.push(label);
    chart.data.datasets.forEach((dataset) => {
        dataset.data.push(data);
    });
    chart.update();
}

function editChartData(chart, labels, data) {
    chart.data.labels = labels;
    chart.data.datasets.forEach((dataset) => {
        dataset.data.push(...data);
    });
    chart.update();
}

function updateChartType(chart, type) {
    const context = chart.ctx;
    const data = chart.data;
    const options = chart.config._config.options;

    if (type != 'bar') {
        delete options.scales
    }

    chart.destroy();
    chart = new Chart(context, {type, data, options});
    return chart;
};

// bootstrap-table helpers /////////////////////////////////////////
function initBootstrapTable($el, rawColumns){
    const columns = rawColumns.map((item) => {
        return {
            field: item,
            title: capitalize(item),
            sortable: true,
        }
    });

    $el.bootstrapTable({
        columns,
        data: [],
        height: 500,
        showFullscreen: true,
        stickyHeader: true,
        stickyHeaderOffsetLeft: 10,
        stickyHeaderOffsetRight: 10,
    });
}

function appendBootstrapRows($el, rows, inplace=false) {
    if (inplace) {
        $el.bootstrapTable('removeAll');
    }
    $el.bootstrapTable('append', rows);
}
