document.addEventListener('DOMContentLoaded', function() {
    // Initialize Bootstrap tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // Loading modal instance
    const loadingModal = new bootstrap.Modal(document.getElementById('loadingModal'));
    
    // Delete venue modal instance
    const deleteVenueModal = new bootstrap.Modal(document.getElementById('deleteVenueModal'));
    
    // Handle scrape venue button clicks
    const scrapeButtons = document.querySelectorAll('.scrape-venue-btn');
    scrapeButtons.forEach(button => {
        button.addEventListener('click', function() {
            const venueId = this.getAttribute('data-venue-id');
            scrapeVenue(venueId);
        });
    });
    
    // Handle scrape all button click
    const scrapeAllBtn = document.getElementById('scrapeAllBtn');
    if (scrapeAllBtn) {
        scrapeAllBtn.addEventListener('click', function() {
            scrapeAllVenues();
        });
    }
    
    // Handle delete venue button clicks
    const deleteButtons = document.querySelectorAll('.delete-venue-btn');
    deleteButtons.forEach(button => {
        button.addEventListener('click', function() {
            const venueId = this.getAttribute('data-venue-id');
            const venueName = this.getAttribute('data-venue-name');
            
            // Set the venue name in the modal
            document.getElementById('venueNameToDelete').textContent = venueName;
            
            // Set up the confirm delete button
            const confirmBtn = document.getElementById('confirmDeleteVenue');
            confirmBtn.onclick = function() {
                deleteVenue(venueId);
            };
            
            // Show the modal
            deleteVenueModal.show();
        });
    });
    
    /**
     * Function to scrape a single venue
     */
    function scrapeVenue(venueId) {
        // Show loading modal
        document.getElementById('loadingMessage').textContent = 'Scraping venue data...';
        loadingModal.show();
        
        // Send API request
        fetch(`/api/venues/${venueId}/scrape`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        })
        .then(response => response.json())
        .then(data => {
            loadingModal.hide();
            
            if (data.status === 'success') {
                // Show success message
                showAlert('success', data.message);
                // Reload page after a short delay
                setTimeout(() => {
                    window.location.reload();
                }, 1500);
            } else {
                // Show error message
                showAlert('danger', data.message || 'An unknown error occurred');
            }
        })
        .catch(error => {
            loadingModal.hide();
            showAlert('danger', `Error: ${error.message}`);
        });
    }
    
    /**
     * Function to scrape all venues
     */
    function scrapeAllVenues() {
        // Show loading modal
        document.getElementById('loadingMessage').textContent = 'Scraping all venues...';
        loadingModal.show();
        
        // Send API request
        fetch('/api/venues/scrape-all', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        })
        .then(response => response.json())
        .then(data => {
            loadingModal.hide();
            
            if (data.status === 'success') {
                // Show success message
                showAlert('success', data.message);
                // Reload page after a short delay
                setTimeout(() => {
                    window.location.reload();
                }, 1500);
            } else {
                // Show error or warning message
                showAlert(data.status === 'warning' ? 'warning' : 'danger', 
                         data.message || 'An unknown error occurred');
            }
        })
        .catch(error => {
            loadingModal.hide();
            showAlert('danger', `Error: ${error.message}`);
        });
    }
    
    /**
     * Function to delete a venue
     */
    function deleteVenue(venueId) {
        // Hide the confirmation modal
        deleteVenueModal.hide();
        
        // Send API request
        fetch(`/api/venues/${venueId}/delete`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                // Reload page
                window.location.reload();
            } else {
                // Show error message
                showAlert('danger', data.message || 'An unknown error occurred');
            }
        })
        .catch(error => {
            showAlert('danger', `Error: ${error.message}`);
        });
    }
    
    /**
     * Utility function to show alert messages
     */
    function showAlert(type, message) {
        // Create alert element
        const alertDiv = document.createElement('div');
        alertDiv.className = `alert alert-${type} alert-dismissible fade show`;
        alertDiv.role = 'alert';
        alertDiv.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        `;
        
        // Add to page
        const mainContent = document.querySelector('main.container');
        mainContent.insertBefore(alertDiv, mainContent.firstChild);
        
        // Auto-dismiss after 5 seconds
        setTimeout(() => {
            const bsAlert = new bootstrap.Alert(alertDiv);
            bsAlert.close();
        }, 5000);
    }
});
