package tjts

const IndexHTML = `
<!DOCTYPE html>
<html>
  <head>
    <title>Triple J in your TZ</title>
    <script type="text/javascript">
      var locTz = Intl.DateTimeFormat().resolvedOptions().timeZone;
      var tstreamURL = window.location.href + "triplej?tz=" + encodeURIComponent(locTz);
      var dstreamURL = window.location.href + "doublej?tz=" + encodeURIComponent(locTz);
      window.onload = function() {
          var tzspan = document.getElementById("detectedtz");
          tzspan.innerHTML = locTz

          var turlspan = document.getElementById("tstreamurl");
          turlspan.innerHTML = tstreamURL;
          var tplayer = document.getElementById("tplayer");
          tplayer.src = tstreamURL;

          var durlspan = document.getElementById("dstreamurl");
          durlspan.innerHTML = dstreamURL;
          var dplayer = document.getElementById("dplayer");
          dplayer.src = dstreamURL;
      }
    </script>
  </head>
  <body>
    <h1>Timezone: <span id="detectedtz"></span></h1>
    <h2>Triple J offset to your TZ</h2>
    <p>Stream URL for detected TZ: <span id="tstreamurl"></span></p>
    <audio id="tplayer" controls></audio>
    <h2>Double J offset to your TZ</h2>
    <p>Stream URL for detected TZ: <span id="dstreamurl"></span></p>
    <audio id="dplayer" controls></audio>
  </body>
</html>
`
