let activeFilter = "all";

const impactEl = document.getElementById("impactChart");
const portfolioEl = document.getElementById("portfolioChart");
const activeFilterLabelEl = document.getElementById("activeFilterLabel");
const clearFilterBtn = document.getElementById("clearFilterBtn");

const donePill = document.getElementById("donePill");
const pausedPill = document.getElementById("pausedPill");
const activePill = document.getElementById("activePill");

const doneValue = document.getElementById("doneValue");
const pausedValue = document.getElementById("pausedValue");
const activeValue = document.getElementById("activeValue");

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function statusLabel(status) {
  if (status === "done") return "Done";
  if (status === "paused") return "Paused";
  if (status === "active") return "Active";
  return "All tasks";
}

function getVisibleTasks() {
  if (activeFilter === "all") return TASKS;
  return TASKS.filter(function(task) {
    return task.status === activeFilter;
  });
}

function toggleFilter(nextFilter) {
  activeFilter = activeFilter === nextFilter ? "all" : nextFilter;
  renderDashboard();
}

function updateStatusPills() {
  const doneCount = TASKS.filter(function(task) { return task.status === "done"; }).length;
  const pausedCount = TASKS.filter(function(task) { return task.status === "paused"; }).length;
  const activeCount = TASKS.filter(function(task) { return task.status === "active"; }).length;

  const total = TASKS.length || 1;
  const donePct = Math.round((doneCount / total) * 100);
  const pausedPct = Math.round((pausedCount / total) * 100);
  const activePct = Math.round((activeCount / total) * 100);

  doneValue.textContent = donePct + "% (" + doneCount + "/" + TASKS.length + ")";
  pausedValue.textContent = pausedPct + "% (" + pausedCount + "/" + TASKS.length + ")";
  activeValue.textContent = activePct + "% (" + activeCount + "/" + TASKS.length + ")";

  donePill.classList.toggle("active", activeFilter === "done");
  pausedPill.classList.toggle("active", activeFilter === "paused");
  activePill.classList.toggle("active", activeFilter === "active");
}

function buildHoverText(task) {
  let statusLine = "Active";
  if (task.status === "done") {
    statusLine = "Done";
  } else if (task.status === "paused") {
    statusLine = "Paused";
  } else {
    statusLine = "Active " + task.activeIndex + "/" + task.activeTotal;
  }

  return "<b>" + escapeHtml(task.name) + "</b><br>" +
         "Owner: " + escapeHtml(task.owner) + "<br>" +
         "Current Impact: " + task.currentImpact + "<br>" +
         "Future Impact: " + task.futureImpact + "<br>" +
         "Progress: " + task.progress + "%<br>" +
         statusLine;
}

function renderImpactChart() {
  const visibleTasks = getVisibleTasks();
  const activeTasks = visibleTasks.filter(function(task) {
    return task.status === "active";
  });

  const haloTrace = {
    x: activeTasks.map(function(task) { return task.currentImpact; }),
    y: activeTasks.map(function(task) { return task.futureImpact; }),
    mode: "markers",
    hoverinfo: "skip",
    showlegend: false,
    marker: {
      size: activeTasks.map(function(task) { return task.bubbleSize + 8; }),
      color: activeTasks.map(function(task) { return task.ownerColor; }),
      opacity: 0.22,
      line: {
        width: 2,
        color: activeTasks.map(function(task) { return task.ownerColor; }),
      },
    },
  };

  const pointTrace = {
    x: visibleTasks.map(function(task) { return task.currentImpact; }),
    y: visibleTasks.map(function(task) { return task.futureImpact; }),
    mode: "markers",
    text: visibleTasks.map(function(task) { return buildHoverText(task); }),
    customdata: visibleTasks.map(function(task) { return task.status; }),
    hovertemplate: "%{text}<extra></extra>",
    showlegend: false,
    marker: {
      size: visibleTasks.map(function(task) { return task.bubbleSize; }),
      color: visibleTasks.map(function(task) { return task.pointColor; }),
      line: { width: 0 },
    },
  };

  const layout = {
    template: "plotly_dark",
    paper_bgcolor: "__BG__",
    plot_bgcolor: "__PANEL_2__",
    font: { color: "__TEXT__" },
    margin: { l: 60, r: 20, t: 10, b: 60 },
    xaxis: {
      title: "Impact on Current Offering (Sales Today)",
      range: [0, 100],
      gridcolor: "__BORDER__",
      zeroline: false,
    },
    yaxis: {
      title: "Impact on Future Offering (Next Packages & Growth)",
      range: [0, 100],
      gridcolor: "__BORDER__",
      zeroline: false,
    },
    hoverlabel: {
      bgcolor: "#111827",
      font: { color: "#f8fafc" },
    },
  };

  Plotly.react(impactEl, [haloTrace, pointTrace], layout, {
    displayModeBar: false,
    responsive: true,
  });

  if (impactEl.removeAllListeners) impactEl.removeAllListeners("plotly_click");
  impactEl.on("plotly_click", function(event) {
    const point = event.points && event.points[0];
    if (!point || !point.customdata) return;
    toggleFilter(point.customdata);
  });
}

function renderPortfolioChart() {
  const doneCount = TASKS.filter(function(task) { return task.status === "done"; }).length;
  const pausedCount = TASKS.filter(function(task) { return task.status === "paused"; }).length;
  const activeCount = TASKS.filter(function(task) { return task.status === "active"; }).length;

  const total = TASKS.length || 1;
  const donePct = Math.round((doneCount / total) * 100);
  const pausedPct = Math.round((pausedCount / total) * 100);
  const activePct = Math.max(0, 100 - donePct - pausedPct);

  const doneTrace = {
    x: [donePct],
    y: [""],
    type: "bar",
    orientation: "h",
    marker: {
      color: "__DONE_GREEN__",
      opacity: activeFilter !== "all" && activeFilter !== "done" ? 0.35 : 1,
    },
    customdata: ["done"],
    hovertemplate: "Done: " + donePct + "% (" + doneCount + "/" + TASKS.length + ")<extra></extra>",
    showlegend: false,
  };

  const pausedTrace = {
    x: [pausedPct],
    y: [""],
    type: "bar",
    orientation: "h",
    marker: {
      color: "__PAUSED_GRAY__",
      opacity: activeFilter !== "all" && activeFilter !== "paused" ? 0.35 : 1,
    },
    customdata: ["paused"],
    hovertemplate: "Paused: " + pausedPct + "% (" + pausedCount + "/" + TASKS.length + ")<extra></extra>",
    showlegend: false,
  };

  const activeTrace = {
    x: [activePct],
    y: [""],
    type: "bar",
    orientation: "h",
    marker: {
      color: "__ACTIVE_BLUE__",
      opacity: activeFilter !== "all" && activeFilter !== "active" ? 0.35 : 1,
    },
    customdata: ["active"],
    hovertemplate: "Active: " + activePct + "% (" + activeCount + "/" + TASKS.length + ")<extra></extra>",
    showlegend: false,
  };

  const layout = {
    barmode: "stack",
    template: "plotly_dark",
    paper_bgcolor: "__BG__",
    plot_bgcolor: "__PANEL_2__",
    font: { color: "__TEXT__" },
    margin: { l: 20, r: 20, t: 10, b: 20 },
    xaxis: {
      range: [0, 100],
      visible: false,
    },
    yaxis: {
      visible: false,
    },
  };

  Plotly.react(portfolioEl, [doneTrace, pausedTrace, activeTrace], layout, {
    displayModeBar: false,
    responsive: true,
  });

  if (portfolioEl.removeAllListeners) portfolioEl.removeAllListeners("plotly_click");
  portfolioEl.on("plotly_click", function(event) {
    const point = event.points && event.points[0];
    if (!point || !point.customdata) return;
    toggleFilter(point.customdata);
  });
}

function renderDashboard() {
  activeFilterLabelEl.textContent = statusLabel(activeFilter);
  updateStatusPills();
  renderImpactChart();
  renderPortfolioChart();
}

clearFilterBtn.addEventListener("click", function() {
  activeFilter = "all";
  renderDashboard();
});

donePill.addEventListener("click", function() {
  toggleFilter("done");
});

pausedPill.addEventListener("click", function() {
  toggleFilter("paused");
});

activePill.addEventListener("click", function() {
  toggleFilter("active");
});

renderDashboard();
