const output = document.getElementById("output");
const jobOutput = document.getElementById("job-output");
const schedulesBody = document.getElementById("schedules-body");
const workflowsBody = document.getElementById("workflows-body");
const datasetSelect = document.getElementById("datasetId");
const scheduleWorkflowSelect = document.getElementById("schedule-workflow-id");
const parameterHelp = document.getElementById("parameter-help");

function write(message, data) {
  const line = data ? `${message}\n${JSON.stringify(data, null, 2)}` : message;
  output.textContent = line;
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(
      data?.detail?.description || response.statusText || "Request failed",
    );
  }

  return data;
}

async function loadCollections() {
  const data = await request("/collections");
  const collections = data.collections || [];
  datasetSelect.innerHTML = "";
  collections.forEach((collection) => {
    const option = document.createElement("option");
    option.value = collection.id;
    option.textContent = `${collection.id} — ${collection.title}`;
    datasetSelect.appendChild(option);
  });

  if (collections.length > 0) {
    datasetSelect.value = collections[0].id;
    await loadParameterHint(collections[0].id);
  }
}

async function loadParameterHint(datasetId) {
  try {
    const cov = await request(`/collections/${datasetId}/coverage`);
    const keys = Object.keys(cov.parameters || {});
    if (keys.length) {
      parameterHelp.textContent = `Available parameters for ${datasetId}: ${keys.join(", ")}`;
      document.getElementById("parameters").value = keys.join(",");
    } else {
      parameterHelp.textContent = `No parameter metadata found for ${datasetId}.`;
    }
  } catch (error) {
    parameterHelp.textContent = `Could not load parameters: ${error.message}`;
  }
}

function scheduleRow(schedule) {
  const tr = document.createElement("tr");
  tr.innerHTML = `
    <td>${schedule.name}</td>
    <td>${schedule.cron} (${schedule.timezone})</td>
    <td>${schedule.enabled ? "yes" : "no"}</td>
    <td>${schedule.lastRunJobId || "-"}</td>
    <td>
      <button data-action="run" data-id="${schedule.scheduleId}">Run now</button>
      <button data-action="callback" data-id="${schedule.scheduleId}">Callback</button>
      <button data-action="toggle" data-id="${schedule.scheduleId}">${schedule.enabled ? "Disable" : "Enable"}</button>
      <button class="danger" data-action="delete" data-id="${schedule.scheduleId}">Delete</button>
    </td>
  `;
  return tr;
}

function workflowRow(workflow) {
  const tr = document.createElement("tr");
  tr.innerHTML = `
    <td>${workflow.name}</td>
    <td>${workflow.steps.length}</td>
    <td>${(workflow.lastRunJobIds || []).join(", ") || "-"}</td>
    <td>
      <button data-workflow-action="run" data-workflow-id="${workflow.workflowId}">Run</button>
      <button data-workflow-action="delete" data-workflow-id="${workflow.workflowId}" class="danger">Delete</button>
    </td>
  `;
  return tr;
}

async function loadWorkflows() {
  const data = await request("/workflows");
  const workflows = data.workflows || [];
  workflowsBody.innerHTML = "";
  scheduleWorkflowSelect.innerHTML =
    '<option value="">Use aggregate-import inputs</option>';

  workflows.forEach((workflow) => {
    workflowsBody.appendChild(workflowRow(workflow));

    const option = document.createElement("option");
    option.value = workflow.workflowId;
    option.textContent = `${workflow.name} (${workflow.steps.length} steps)`;
    scheduleWorkflowSelect.appendChild(option);
  });
}

async function createWorkflow(event) {
  event.preventDefault();

  const name = document.getElementById("workflow-name").value.trim();
  const stepsRaw = document.getElementById("workflow-steps").value.trim();
  if (!name) {
    write("Workflow name is required");
    return;
  }

  let steps;
  try {
    steps = JSON.parse(stepsRaw);
  } catch (error) {
    write(`Invalid workflow JSON: ${error.message}`);
    return;
  }

  if (!Array.isArray(steps) || steps.length === 0) {
    write("Workflow steps must be a non-empty array");
    return;
  }

  const invalidStep = steps.find(
    (step) =>
      !step || typeof step !== "object" || !step.processId || !step.payload,
  );
  if (invalidStep) {
    write(
      "Each step must include processId and payload. Optional fields: name",
      invalidStep,
    );
    return;
  }

  const workflow = {
    name,
    steps,
  };

  const created = await request("/workflows", {
    method: "POST",
    body: JSON.stringify(workflow),
  });

  await loadWorkflows();
  write("Workflow saved", created);
}

async function runWorkflow(workflowId) {
  const result = await request(`/workflows/${workflowId}/run`, {
    method: "POST",
  });

  await loadWorkflows();
  write("Workflow run submitted", {
    workflowId,
    jobIds: result.jobIds,
  });

  if ((result.jobIds || []).length > 0) {
    document.getElementById("job-id").value =
      result.jobIds[result.jobIds.length - 1];
  }
}

async function deleteWorkflow(workflowId) {
  await request(`/workflows/${workflowId}`, {
    method: "DELETE",
  });

  await loadWorkflows();
  write("Workflow deleted", { workflowId });
}

async function loadSchedules() {
  const data = await request("/schedules");
  schedulesBody.innerHTML = "";
  (data.schedules || []).forEach((schedule) => {
    schedulesBody.appendChild(scheduleRow(schedule));
  });
}

async function createSchedule(event) {
  event.preventDefault();

  const workflowId = scheduleWorkflowSelect.value || null;
  const payload = {
    name: document.getElementById("name").value,
    cron: document.getElementById("cron").value,
    timezone: document.getElementById("timezone").value,
    enabled: true,
  };

  if (workflowId) {
    payload.workflowId = workflowId;
  } else {
    payload.inputs = {
      datasetId: datasetSelect.value,
      parameters: document
        .getElementById("parameters")
        .value.split(",")
        .map((x) => x.trim())
        .filter(Boolean),
      datetime: document.getElementById("datetime").value,
      orgUnitLevel: Number(document.getElementById("orgUnitLevel").value),
      aggregation: document.getElementById("aggregation").value,
      dhis2: {
        dataElementId: document.getElementById("dataElementId").value,
        dryRun: document.getElementById("dryRun").checked,
      },
    };
  }

  try {
    const data = await request("/schedules", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    write("Schedule created", data);
    await loadSchedules();
  } catch (error) {
    write(`Failed to create schedule: ${error.message}`);
  }
}

async function runSchedule(scheduleId) {
  const data = await request(`/schedules/${scheduleId}/run`, {
    method: "POST",
  });
  write("Schedule started", data);
  document.getElementById("job-id").value = data.jobId;
  await loadSchedules();
}

async function callbackSchedule(scheduleId) {
  const token = document.getElementById("scheduler-token").value;
  const headers = token ? { "X-Scheduler-Token": token } : {};
  const data = await request(`/schedules/${scheduleId}/callback`, {
    method: "POST",
    headers,
  });
  write("Scheduler callback started", data);
  document.getElementById("job-id").value = data.jobId;
  await loadSchedules();
}

async function toggleSchedule(scheduleId) {
  const current = await request(`/schedules/${scheduleId}`);
  const data = await request(`/schedules/${scheduleId}`, {
    method: "PATCH",
    body: JSON.stringify({ enabled: !current.enabled }),
  });
  write("Schedule updated", data);
  await loadSchedules();
}

async function deleteSchedule(scheduleId) {
  await request(`/schedules/${scheduleId}`, { method: "DELETE" });
  write("Schedule deleted");
  await loadSchedules();
}

async function checkJob() {
  const jobId = document.getElementById("job-id").value.trim();
  if (!jobId) {
    jobOutput.textContent = "Provide a jobId";
    return;
  }

  try {
    const data = await request(`/jobs/${jobId}`);
    jobOutput.textContent = JSON.stringify(data, null, 2);
  } catch (error) {
    jobOutput.textContent = `Failed to load job: ${error.message}`;
  }
}

document
  .getElementById("schedule-form")
  .addEventListener("submit", createSchedule);
document
  .getElementById("workflow-form")
  .addEventListener("submit", createWorkflow);
document
  .getElementById("refresh-schedules")
  .addEventListener("click", loadSchedules);
document
  .getElementById("refresh-workflows")
  .addEventListener("click", loadWorkflows);
document.getElementById("check-job").addEventListener("click", checkJob);
datasetSelect.addEventListener("change", (event) =>
  loadParameterHint(event.target.value),
);

workflowsBody.addEventListener("click", async (event) => {
  const button = event.target.closest("button");
  if (!button) return;

  const action = button.dataset.workflowAction;
  const workflowId = button.dataset.workflowId;

  try {
    if (action === "run") await runWorkflow(workflowId);
    if (action === "delete") await deleteWorkflow(workflowId);
  } catch (error) {
    write(`Workflow action failed: ${error.message}`);
  }
});

schedulesBody.addEventListener("click", async (event) => {
  const button = event.target.closest("button");
  if (!button) return;

  const action = button.dataset.action;
  const scheduleId = button.dataset.id;

  try {
    if (action === "run") await runSchedule(scheduleId);
    if (action === "callback") await callbackSchedule(scheduleId);
    if (action === "toggle") await toggleSchedule(scheduleId);
    if (action === "delete") await deleteSchedule(scheduleId);
  } catch (error) {
    write(`Action failed: ${error.message}`);
  }
});

(async () => {
  try {
    await loadCollections();
    await loadWorkflows();
    await loadSchedules();
  } catch (error) {
    write(`Initialization failed: ${error.message}`);
  }
})();
