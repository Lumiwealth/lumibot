function loadFile(fileName) {
    return $.getJSON(fileName, function(data) {
        return data;
    });
}

function addDay(date, increment) {
    const day = date.getDate();
    const month = date.getMonth();
    const year = date.getFullYear();

    const newDate = new Date(year, month, day);
    newDate.setDate(day + increment);
    return newDate;
};

function getDates(startDate, endDate) {
    const dates = [];
    let currentDate = startDate;

    while (currentDate <= endDate) {
        dates.push(currentDate);
        currentDate = addDay(currentDate, 1);
    }

    return dates;
};

function seedPortfolioData() {
    return {
        "portfolio_value": 29523,
        "unspent_money": 1200,
        "time": new Date(2013,12,5),
        "positions": [
            {
                "symbol": "SPY",
                "quantity": 100,
                "price": 52.60,
            },
            {
                "symbol": "GOOG",
                "quantity": 150,
                "price": 78.90,
            },
            {
                "symbol": "AAPL",
                "quantity": 80,
                "price": 140.35,
            }
        ]
    };
}

function seedPortfolioEvolutionData() {
    const dates = getDates(new Date(2013,10,22), new Date(2013,12,4));
    const labels = dates.map(item => item.getTime());
    const values = [...Array(labels.length)].map(e => Math.random()*100000);
    return {labels, values};
}

function seedLogMessages(){
    return [
        {
            time: "2021-04-18T07:12:43.078Z",
            message: "This is a log message with id 19de7bb5-5408-4a51-9a6e-77e3b7047423",
        },
        {
            time: "2021-04-18T07:52:43.078Z",
            message: "This is a log message with id df49b961-18f7-4c30-afc6-d719ca79c867",
        },
        {
            time: "2021-04-18T08:42:43.078Z",
            message: "This is a log message with id 7501519e-f819-4193-a7bf-b1a5387b51aa",
        },
        {
            time: "2021-04-18T09:10:43.078Z",
            message: "This is a log message with id 35c9c99a-bede-4d15-bd53-347a39d848f2",
        },
        {
            time: "2021-04-18T09:17:43.078Z",
            message: "This is a log message with id f5843ba6-77d1-412a-b965-1235a1054016",
        },
        {
            time: "2021-04-18T10:37:43.078Z",
            message: "This is a log message with id f7b843c3-1ba3-4b77-a7ec-264c0cdc6c15",
        },
        {
            time: "2021-04-18T10:55:43.078Z",
            message: "This is a log message with id 7bbc143a-7735-42ab-8088-821bdc0dad9a",
        },
        {
            time: "2021-04-18T11:15:43.078Z",
            message: "This is a log message with id 90debd92-e40a-4c3f-b713-f2218e64ef7b",
        },
        {
            time: "2021-04-18T11:40:43.078Z",
            message: "This is a log message with id 92f24ab0-e3fc-4fa8-a34b-677a5e9f765f",
        },
        {
            time: "2021-04-18T12:01:43.078Z",
            message: "This is a log message with id 65bfcac7-a135-4941-b630-d41745be21cd",
        },
        {
            time: "2021-04-18T14:20:43.078Z",
            message: "This is a log message with id 95f5e7d8-a13e-11eb-bcbc-0242ac130002",
        },
    ];
}

function seedData(){
    return new Promise((resolve, reject) => {
        resolve(loadFile("static/resources/stats_example.json").then((resp) => {
            return {
                lastPortfolioData: seedPortfolioData(),
                portfolioGraphPoints: seedPortfolioEvolutionData(),
                statRows: resp.sample,
                logRows: seedLogMessages(),
            };
        }));
    });
}
