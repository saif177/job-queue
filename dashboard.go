package queue

import "net/http"

func dashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(dashboardHTML))
}

// dashboardHTML is a dependency-free single page that talks to the JSON API.
// Job fields are inserted with textContent, never innerHTML, so job data
// cannot inject markup.
const dashboardHTML = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Job Queue</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
body{font:14px system-ui,sans-serif;margin:2rem auto;max-width:960px;padding:0 1rem}
table{border-collapse:collapse;width:100%}th,td{padding:.4rem .6rem;border-bottom:1px solid #ddd;text-align:left}
.s-pending{color:#b58900}.s-running{color:#268bd2}.s-succeeded{color:#2a9d3f}.s-failed,.s-dead{color:#c0392b}.s-cancelled{color:#888}
button{margin-right:.3rem}#err{color:#c0392b}
</style></head><body>
<h1>Job Queue</h1>
<p>Filter: <select id="filter"></select> <span id="err"></span></p>
<table><thead><tr><th>ID</th><th>Type</th><th>Status</th><th>Priority</th><th>Attempts</th><th>Error</th><th></th></tr></thead>
<tbody id="rows"></tbody></table>
<script>
const statuses=["","pending","running","succeeded","failed","dead","cancelled"];
const filter=document.getElementById("filter"),rows=document.getElementById("rows"),err=document.getElementById("err");
for(const s of statuses){const o=document.createElement("option");o.value=s;o.textContent=s||"all";filter.append(o)}
function cell(tr,text,cls){const td=tr.insertCell();td.textContent=text;if(cls)td.className=cls;return td}
async function act(id,what){
  const r=await fetch("/jobs/"+id+"/"+what,{method:"POST"});
  if(!r.ok){err.textContent=(await r.json()).error}else{err.textContent=""}
  load();
}
function button(td,label,id,what){const b=document.createElement("button");b.textContent=label;b.onclick=()=>act(id,what);td.append(b)}
async function load(){
  const r=await fetch("/jobs?status="+encodeURIComponent(filter.value));
  const {jobs}=await r.json();
  rows.replaceChildren();
  for(const j of jobs){
    const tr=rows.insertRow();
    cell(tr,j.id);cell(tr,j.type||"(func)");cell(tr,j.status,"s-"+j.status);
    cell(tr,j.priority);cell(tr,j.attempts);cell(tr,j.error||"");
    const td=tr.insertCell();
    if(j.status==="pending")button(td,"Cancel",j.id,"cancel");
    if(["failed","dead","cancelled"].includes(j.status))button(td,"Retry",j.id,"retry");
  }
}
filter.onchange=load;load();setInterval(load,3000);
</script></body></html>`
