window.addEventListener('DOMContentLoaded', (event) => {

  const produceAllButton = document.getElementById("produce-all");
  const consumeAllButton = document.getElementById("consume-all");
  const stopProduceAllButton = document.getElementById("stop-producing-all");
  const stopConsumeAllButton = document.getElementById("stop-consuming-all");
  const startWandbButton = document.getElementById("start-wandb");



  produceAllButton.addEventListener("click", function() {
      fetch("/produce-all", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

  consumeAllButton.addEventListener("click", function() {
      fetch("/consume-all", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

  stopProduceAllButton.addEventListener("click", function() {
      fetch("/stop-producing-all", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

  stopConsumeAllButton.addEventListener("click", function() {
      fetch("/stop-consuming-all", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

  startWandbButton.addEventListener("click", function() {
      fetch("/start-wandb", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

});