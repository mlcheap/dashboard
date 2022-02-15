function load_table(elem,data) {
  if (Object.keys(data).length==0) {
    elem.html('');
  }
  elem.html('');
  let table = elem.append("table");
  table
    .append("tr")
    .selectAll("th")
    .data(Object.keys(data[0]))
    .enter()
    .append("th")
    .text(k => k);
  data.forEach((d,i) => {
    table.append('tr')
      .selectAll("td")
      .data(Object.values(d))
      .enter()
      .append("td")
      .text(v => v);
  });
}


function review(task_ids, elem) {
  function load_task(task_id) {
    let e = d3.select(elem);
    d3.select('.pagination a.active').classed('active',false);
    d3.select(`#task${task_id}`).node().classList.toggle("active");
    // d3.select('.task').html('<div class="loader"></div>');
    d3.json(`/dashboard/get-task?task_id=${task_id}`, (response) => {
      d3.select('.task').html(`
        <ul class="tags"></ul>
        <h3 class="title"></h3>
        <p class="description"></p>`);
      d3.select('.task .title')
        .text(response.task.title);
      d3.select('.task .description')
        .text(response.task.description);
      d3.select('.task .tags')
        .html("")
        .selectAll('li')
        .data(response.tags)
        .enter()
        .append('li')
        .attr('class','button')
      // .on('click',function() { this.classList.toggle('active') })
        .html(tag => `
          ${tag.title}
          <i class="fa fa-question-circle tooltip" id="${tag.occupation_id}">
          <d class="tooltiptext">
          <h4>Annotators</h4> 
          <p>${ tag.emails.reduce((p,v)=>p + " " + v,"") }</p>
          <a href="${tag.external_id}">ESCO resource link</a>
          </d>
          </i>`);
    });
  }

  function prev() {
    let a = document.querySelector('.pagination a.active');
    let b = a.previousSibling;
    if (b) {
      a.classList.remove('active');
      b.classList.add('active');
      let task_id = b.getAttribute('id').slice(4);
      load_task(task_id);
    }
  }

  function next() {
    let a = document.querySelector('.pagination a.active');
    let b = a.nextSibling;
    if (b) {
      a.classList.remove('active');
      b.classList.add('active');
      let task_id = b.getAttribute('id').slice(4);
      load_task(task_id);
    }
  }
  window.onkeydown = function(e) {
    if (e.key=='ArrowRight') { next() } 
    else if (e.key=='ArrowLeft') { prev() }
  }
  d3.select(elem)
    // .attr('dir','rtl')
    .html(`
      <p dir='ltr'>
      Hover over <i class='fa fa-question-circle tooltip'></i> to view annotators who confirmed each tag. 
      Navigage tasks by <a href='#' onclick="prev();">&laquo;</a> 
      and <a href='#' onclick="next();">&raquo;</a> keys. 
      </p>
      <div class="pagination"></div>
      <div class="task"></div>`);
  task_ids.forEach((task_id,it) => {
    d3.select('.pagination')
      .append('a')
      .text(it+1)
      .attr('href','#')
      .attr('id',`task${task_id}`)
      .on("click",() => load_task(task_id));
  });
  load_task(task_ids[0]);
}

function modal(elem) {
  d3.select(elem)
    .html(`
      <div class="modal">
        <div class="modal-content">
          <span class="close">&times;</span>
          <div class="modal-content-body"></div>
        </div>
      </div>
      `);
  d3.select(`${elem} .close`).on('click', () => d3.select(elem).select(`.modal`).style('display', "none"));
}


function load_index(client_name,freq) {
  function load_proj(response,proj) {
    $("#users").html('<div class="loader"></div>');
    $(".table-stats").html('<div class="loader"></div>');
    $(".menu-item").removeClass("active");
    $(`#${proj}.menu-item`).addClass("active");
    let proj_data = response.data.filter((a)=>a.project_name==proj)
    let total = proj_data.reduce((S,a)=>S+a.labels,0);

    d3.select(`.review-button`).on('click', () => {
      d3.select(`#modal .modal`).style('display', "block");
      d3.select('#modal .modal-content-body').html('<div class="loader"></div>');
      d3.json(`/dashboard/inconsistent-tasks?project_name=${proj}`,function(response) {
        task_ids = response.data.map((a) => a.task_id);
        review(task_ids=task_ids,elem=`#modal .modal-content-body`)
      });
    });
    d3.json(`/dashboard/user-stats?project_name=${proj}&formatted=1`,response => {
      load_table(d3.select(`.table-stats`),response.data);
    });
    
    d3.json(`/dashboard/users-activity?freq=${freq}&project_name=${proj}`,response => {
      traces = [];
      response.data.forEach(data => traces.push({
        type: "bar",
        mode: "lines",
        name: data.email,
        x: data.inserted_at,
        y: data.labels,
      }));


      var data = traces;

      var layout = {
        title: 'Number of labels/30 minutes',
        barmode: 'stack',
        xaxis: {
          autorange: true,
          rangeselector: {buttons: [
              {step: 'all'},
              {
                count: 7,
                label: 'last week',
                step: 'day',
                stepmode: 'backward'
              },
              {
                count: 1,
                label: 'last day',
                step: 'day',
                stepmode: 'backward'
              }
            ]},
          rangeslider: {},
          type: 'date'
        },
        yaxis: {
          autorange: true,
          type: 'linear'
        }
      };
      
      var config = {responsive: true}

      $('#users').html('');
      Plotly.newPlot('users', data, layout,config);
    });
     
  }
  window.onclick = function(event) {
    if (event.target.className == 'modal') {
      d3.selectAll('.modal').style('display', "none")
    }
    if (!event.target.closest('.tooltiptext') & !event.target.classList.contains('tooltip')) {
      d3.selectAll('i').classed('active',false);
    }
  }
  d3.select('head title').text(`Dashboard ${client_name}`);
  d3.select('body').html(`
  <ul class="menu"></ul>
  <div class="content">
    <h2>Summary</h2>
    <div class="table-stats"></div>
    <canvas id="myChart"></canvas>
    <h2>Review</h2>
    <button class="button review-button">Review ambiguous tasks</button>
    These are tasks with at least one contested tag. 
    <h2>User activities</h2>
    <div id="users" style="width: 90vw;"></div>
    <div id='modal'></div>
  <div>
  `);
  modal(elem=`#modal`);
  d3.json(`/dashboard/get-projects?client_name=${client_name}`,function(projs) {
    d3.json(`/dashboard/project-stats?client_name=${client_name}`,function(response) {
      projs.forEach((proj) => {
        $(".menu").append($(`<li><a class="menu-item" id="${proj}" href="#">${proj}</a></li>`).click(function() {
          load_proj(response,proj);
        }));
      });
      load_proj(response, projs[0]);
    });
  });


}

const CHART_COLORS = {
  red: 'rgb(255, 99, 132)',
  orange: 'rgb(255, 159, 64)',
  yellow: 'rgb(255, 205, 86)',
  green: 'rgb(75, 192, 192)',
  blue: 'rgb(54, 162, 235)',
  purple: 'rgb(153, 102, 255)',
  grey: 'rgb(201, 203, 207)'
};

function chart() {
  let proj = 'SAU3';
  $.getJSON(`/dashboard/user-stats?project_name=${proj}`, response => {
    console.log(response);
    let datasets = [];
    const colors = [CHART_COLORS.red,CHART_COLORS.blue,CHART_COLORS.green, CHART_COLORS.yellow];
    let labels = ['search','top_5','top_10','top_20','top_50'];
    response.data.forEach((record,i) => {
      let data = labels.map(a=>record[a]);
      console.log(record,data);
      datasets.push({
        label: record.email,
        data: data,
        fill: true,
        backgroundColor: colors[i],
      });
    });
    let emails = response.data.map( a => a.email);
    let data = {labels: labels, datasets: datasets};
    const config = {
      type: 'bar',
      data: data,
      options: {
        plugins: {
          title: {
            display: true,
            text: 'Chart.js Bar Chart - Stacked'
          },
        },
        responsive: true,
        scales: {
          x: {
            stacked: true,
          },
          y: {
            stacked: true
          }
        }
      }
    };

    
    $('body').html(`<canvas id="myChart"></canvas>`);
    const myChart = new Chart(
      document.getElementById('myChart'),
      config
    );
  })


}

