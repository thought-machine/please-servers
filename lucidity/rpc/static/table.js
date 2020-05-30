google.charts.load('current', {'packages':['table']});
google.charts.setOnLoadCallback(drawTable);

function drawTable() {
    var dt = new google.visualization.DataTable();
    dt.addColumn('string', 'Name');
    dt.addColumn('boolean', 'Alive');
    dt.addColumn('boolean', 'Healthy');
    dt.addColumn('boolean', 'Free');
    dt.addColumn('string', 'Uptime');
    dt.addColumn('string', 'Last Update');
    dt.addColumn('string', 'Last Task');
    dt.addColumn('string', 'Status');

    $.get('/workers', function(data) {
        dt.addRows(data.workers.map(w => [
            w.name,
            w.alive,
            w.healthy,
            w.healthy && !w.busy,
            moment.duration(moment().diff(moment.unix(w.start_time)), "milliseconds").format("d[d] h[h] m[m] s[s]", {trim: 'both'}),
            moment.duration(moment().diff(moment.unix(w.update_time)), "milliseconds").format("d[d] h[h] m[m] s[s]", {trim: 'both'}),
            w.last_task ? `<a href="${w.last_task}">Last task</a>` : '',
            w.status,
        ]));

        var table = new google.visualization.Table(document.getElementById('table'));
        table.draw(dt, {
            showRowNumber: true,
            width: '100%',
            height: '100%',
            sortColumn: 0,
            allowHtml: true,
            alternatingRowStyle: false,
            cssClassNames: {
                headerRow: 'table-header',
                tableRow:  'table-row',
            },
        });

        google.visualization.events.addListener(table, 'sort', addStyles);
        addStyles();

        // Populate the overview stats
        const total = data.workers.length;
        const healthy = data.workers.reduce((t, w) => t + (w.healthy ? 1 : 0), 0);
        const alive = data.workers.reduce((t, w) => t + (w.alive ? 1 : 0), 0);
        const busy = data.workers.reduce((t, w) => t + (w.busy ? 1 : 0), 0);
        document.getElementById('total').textContent = total;
        document.getElementById('healthy').textContent = healthy;
        document.getElementById('unhealthy').textContent = total - healthy;
        document.getElementById('down').textContent = total - alive;
        document.getElementById('busy').textContent = busy;
        document.getElementById('free').textContent = total - busy;
    });
}

function addStyles() {
    // Bit of a hack to get styling on the rows
    $('tr').addClass('dead');
    $('td:contains(✔):nth-child(3)').parent().addClass('alive unhealthy').removeClass('dead');
    $('td:contains(✔):nth-child(4)').parent().addClass('healthy').removeClass('alive dead unhealthy');
    $('td:contains(✔):nth-child(5)').parent().addClass('free').removeClass('alive healthy');
    $('td:contains(✔)').addClass('tick');
}
