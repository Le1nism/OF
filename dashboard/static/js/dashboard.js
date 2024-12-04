window.addEventListener('DOMContentLoaded', (event) => {

  const produceAllButton = document.getElementById("produce-all");
  produceAllButton.addEventListener("click", function() {
      fetch("/produce-all", {method: "POST"})
        .then(response => response.text())
        .then(data => console.log(data));
  });
  
});