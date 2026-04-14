/**
 * Dashboard background: CSS spotlight/parallax + sparse star net (nodes + links; follows pointer).
 */
(function () {
  var body = document.body;
  if (!body.classList.contains("dashboard-dynamic-bg")) {
    return;
  }

  var reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  var targetSpotX = 50;
  var targetSpotY = 50;
  var curSpotX = 50;
  var curSpotY = 50;
  var targetPx = 0;
  var targetPy = 0;
  var curPx = 0;
  var curPy = 0;

  var targetMx = window.innerWidth * 0.5;
  var targetMy = window.innerHeight * 0.5;
  var smoothMx = targetMx;
  var smoothMy = targetMy;

  var canvas = document.querySelector(".dynamic-bg-star-net");
  var ctx = canvas && canvas.getContext && canvas.getContext("2d");
  var nodes = [];
  /** Larger = fewer neighbour links (less dense net) */
  var connectDist = 128;
  var cursorLinks = 5;
  var loopOn = false;

  function setVars() {
    body.style.setProperty("--spot-x", curSpotX + "%");
    body.style.setProperty("--spot-y", curSpotY + "%");
    body.style.setProperty("--parallax-x", String(curPx));
    body.style.setProperty("--parallax-y", String(curPy));
  }

  function rebuildNodes(w, h) {
    nodes = [];
    var cols = Math.max(5, Math.round(w / 145));
    var rows = Math.max(4, Math.round(h / 145));
    var pad = 0.05;
    var r;
    var c;
    for (r = 0; r < rows; r++) {
      for (c = 0; c < cols; c++) {
        var bx =
          pad +
          (1 - 2 * pad) * ((c + 0.5) / cols) +
          (Math.random() - 0.5) * (0.06 / cols);
        var by =
          pad +
          (1 - 2 * pad) * ((r + 0.5) / rows) +
          (Math.random() - 0.5) * (0.06 / rows);
        nodes.push({ bx: bx, by: by, phase: Math.random() * Math.PI * 2 });
      }
    }
  }

  function resizeCanvas() {
    if (!canvas || !ctx) {
      return;
    }
    var w = window.innerWidth;
    var h = window.innerHeight;
    var dpr = Math.min(window.devicePixelRatio || 1, 2);
    canvas.width = Math.floor(w * dpr);
    canvas.height = Math.floor(h * dpr);
    canvas.style.width = w + "px";
    canvas.style.height = h + "px";
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    rebuildNodes(w, h);
  }

  function nodePos(n, w, h, smx, smy, t) {
    var baseX = n.bx * w;
    var baseY = n.by * h;
    var dx = smx - baseX;
    var dy = smy - baseY;
    var dist = Math.hypot(dx, dy) + 90;
    var panX = (smx - w * 0.5) * 0.1;
    var panY = (smy - h * 0.5) * 0.09;
    var pull = Math.min(200 / dist, 0.42) * 44;
    var x = baseX + panX + (dx / dist) * pull;
    var y = baseY + panY + (dy / dist) * pull;
    x += Math.sin(t * 0.00065 + n.phase) * 1.4;
    y += Math.cos(t * 0.0006 + n.phase) * 1.4;
    return { x: x, y: y };
  }

  function drawStarNet(w, h, smx, smy, t) {
    if (!ctx || !nodes.length) {
      return;
    }
    ctx.clearRect(0, 0, w, h);

    var pos = [];
    var i;
    var j;
    for (i = 0; i < nodes.length; i++) {
      pos.push(nodePos(nodes[i], w, h, smx, smy, t));
    }

    for (i = 0; i < pos.length; i++) {
      for (j = i + 1; j < pos.length; j++) {
        var d = Math.hypot(pos[i].x - pos[j].x, pos[i].y - pos[j].y);
        if (d < connectDist) {
          var midX = (pos[i].x + pos[j].x) * 0.5;
          var midY = (pos[i].y + pos[j].y) * 0.5;
          var midToC = Math.hypot(midX - smx, midY - smy);
          var baseA = (1 - d / connectDist) * 0.16;
          var boost = Math.max(0, 1 - midToC / 320) * 0.18;
          var a = Math.min(baseA + boost, 0.42);
          ctx.beginPath();
          ctx.moveTo(pos[i].x, pos[i].y);
          ctx.lineTo(pos[j].x, pos[j].y);
          ctx.strokeStyle = "rgba(94, 234, 212, " + a + ")";
          ctx.lineWidth = 0.65 + boost * 0.45;
          ctx.stroke();
        }
      }
    }

    var dists = [];
    for (i = 0; i < pos.length; i++) {
      dists.push({
        i: i,
        d: Math.hypot(pos[i].x - smx, pos[i].y - smy),
      });
    }
    dists.sort(function (a, b) {
      return a.d - b.d;
    });
    var k = Math.min(cursorLinks, dists.length);
    for (j = 0; j < k; j++) {
      var idx = dists[j].i;
      var dd = dists[j].d + 1;
      var ca = Math.min(0.55, 0.22 + 140 / dd);
      ctx.beginPath();
      ctx.moveTo(smx, smy);
      ctx.lineTo(pos[idx].x, pos[idx].y);
      ctx.strokeStyle = "rgba(165, 243, 252, " + ca + ")";
      ctx.lineWidth = 0.95;
      ctx.stroke();
    }

    for (i = 0; i < pos.length; i++) {
      var dc = Math.hypot(pos[i].x - smx, pos[i].y - smy);
      var starA = 0.28 + Math.max(0, 1 - dc / 240) * 0.38;
      ctx.beginPath();
      ctx.arc(pos[i].x, pos[i].y, 1.35 + (1 - Math.min(dc, 220) / 220) * 0.55, 0, Math.PI * 2);
      ctx.fillStyle = "rgba(226, 232, 240, " + starA + ")";
      ctx.fill();
    }

    ctx.beginPath();
    ctx.arc(smx, smy, 2.4, 0, Math.PI * 2);
    ctx.fillStyle = "rgba(204, 251, 241, 0.55)";
    ctx.fill();
    ctx.beginPath();
    ctx.arc(smx, smy, 5.5, 0, Math.PI * 2);
    ctx.strokeStyle = "rgba(94, 234, 212, 0.22)";
    ctx.lineWidth = 1;
    ctx.stroke();
  }

  function onPointer(e) {
    var w = window.innerWidth || 1;
    var h = window.innerHeight || 1;
    targetSpotX = (e.clientX / w) * 100;
    targetSpotY = (e.clientY / h) * 100;
    targetPx = (e.clientX / w - 0.5) * 2;
    targetPy = (e.clientY / h - 0.5) * 2;
    targetMx = e.clientX;
    targetMy = e.clientY;
  }

  function frame(t) {
    if (!loopOn) {
      return;
    }

    var ease = reduceMotion ? 1 : 0.08;
    curSpotX += (targetSpotX - curSpotX) * ease;
    curSpotY += (targetSpotY - curSpotY) * ease;
    curPx += (targetPx - curPx) * ease;
    curPy += (targetPy - curPy) * ease;
    smoothMx += (targetMx - smoothMx) * 0.1;
    smoothMy += (targetMy - smoothMy) * 0.1;
    setVars();

    if (!reduceMotion && ctx) {
      drawStarNet(window.innerWidth, window.innerHeight, smoothMx, smoothMy, t);
    }

    requestAnimationFrame(frame);
  }

  function start() {
    if (reduceMotion) {
      setVars();
      return;
    }
    if (canvas && ctx) {
      resizeCanvas();
      window.addEventListener("resize", resizeCanvas, { passive: true });
    }
    window.addEventListener("pointermove", onPointer, { passive: true });
    onPointer({
      clientX: window.innerWidth * 0.5,
      clientY: window.innerHeight * 0.5,
    });
    curSpotX = targetSpotX;
    curSpotY = targetSpotY;
    curPx = targetPx;
    curPy = targetPy;
    smoothMx = targetMx;
    smoothMy = targetMy;
    setVars();
    loopOn = true;
    requestAnimationFrame(frame);
  }

  start();
})();
