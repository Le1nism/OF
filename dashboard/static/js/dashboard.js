window.addEventListener('DOMContentLoaded', (event) => {

  const produceAllButton = document.getElementById("produce-all");
  const consumeAllButton = document.getElementById("consume-all");
  const stopProduceAllButton = document.getElementById("stop-producing-all");
  const stopConsumeAllButton = document.getElementById("stop-consuming-all");
  const startWandbButton = document.getElementById("start-wandb");
  const stopWandbButton = document.getElementById("stop-wandb");
  const createVehiclesButton = document.getElementById("create-vehicles");
  const deleteVehiclesButton = document.getElementById("delete-vehicles");
  const startFederatedLearningButton = document.getElementById("start-federated-learning");
  const stopFederatedLearningButton = document.getElementById("stop-federated-learning");
  const startSecurityManagerButton = document.getElementById("start-security-manager");
  const stopSecurityManagerButton = document.getElementById("stop-security-manager");
  const startAutomaticAttacksButton = document.getElementById("start-automatic-attacks");
  const stopAutomaticAttacksButton = document.getElementById("stop-automatic-attacks");
  const startAttackButtons = Array.from(document.querySelectorAll('[id$="_start_attack"]'));
  const stopAttackButtons = Array.from(document.querySelectorAll('[id$="_end_attack"]'))
  const startPreconfAttackButton = document.getElementById("start-preconf-attack");
  const stopPreconfAttackButton = document.getElementById("stop-preconf-attack");



  startAutomaticAttacksButton.addEventListener("click", function() {
      fetch("/start-automatic-attacks", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

  stopAutomaticAttacksButton.addEventListener("click", function() {
      fetch("/stop-automatic-attacks", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

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

  stopWandbButton.addEventListener("click", function() {
      fetch("/stop-wandb", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

  createVehiclesButton.addEventListener("click", function() {
      fetch("/create-vehicles", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

  deleteVehiclesButton.addEventListener("click", function() {
      fetch("/delete-vehicles", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

  startFederatedLearningButton.addEventListener("click", function() {
      fetch("/start-federated-learning", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

  stopFederatedLearningButton.addEventListener("click", function() {
      fetch("/stop-federated-learning", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

  startSecurityManagerButton.addEventListener("click", function() {
      fetch("/start-security-manager", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

  stopSecurityManagerButton.addEventListener("click", function() {
      fetch("/stop-security-manager", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

  startPreconfAttackButton.addEventListener("click", function() {
      fetch("/start-preconf-attack", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

  stopPreconfAttackButton.addEventListener("click", function() {
      fetch("/stop-preconf-attack", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });

  startAttackButtons.forEach(button => {
      button.addEventListener("click", function() {
          fetch("/start-attack", {
              method: "POST",
              headers: {
                  'Content-Type': 'application/json'
              },
              body: JSON.stringify({ vehicle_name: button.id })
          })
          .then(response => response.text())
          .then(data => console.log(data));
      });
  });

  stopAttackButtons.forEach(button => {
      button.addEventListener("click", function() {
          fetch("/stop-attack", {
              method: "POST",
              headers: {
                  'Content-Type': 'application/json'
              },
              body: JSON.stringify({ vehicle_name: button.id,
                                      origin: "MANUALLY"
                                    })
          })
          .then(response => response.text())
          .then(data => console.log(data));
      });
  });

});