// Tusk Profile Page JavaScript

let currentUser = null;

document.addEventListener('DOMContentLoaded', loadProfile);

async function loadProfile() {
    try {
        const res = await fetch('/api/profile');
        const data = await res.json();

        if (data.error) {
            showToast(data.error, 'error');
            window.location.href = '/login';
            return;
        }

        currentUser = data.user;

        // Fill form
        document.getElementById('profile-username').value = data.user.username;
        document.getElementById('profile-display-name').value = data.user.display_name || '';
        document.getElementById('profile-email').value = data.user.email || '';

        // Show groups
        const groupsEl = document.getElementById('user-groups');
        if (data.groups && data.groups.length > 0) {
            groupsEl.innerHTML = data.groups.map(g =>
                `<span class="px-3 py-1 bg-indigo-500/20 text-indigo-400 rounded-full text-sm">${g.name}</span>`
            ).join('');
        } else {
            groupsEl.innerHTML = '<span class="text-gray-500 text-sm">No groups assigned</span>';
        }

        // Show permissions
        const permsEl = document.getElementById('user-permissions');
        if (data.permissions && data.permissions.length > 0) {
            permsEl.innerHTML = data.permissions.map(p =>
                `<span class="px-2 py-0.5 bg-[#21262d] text-gray-400 rounded text-xs font-mono">${p}</span>`
            ).join('');
        } else {
            permsEl.innerHTML = '<span class="text-gray-500 text-sm">No permissions</span>';
        }

    } catch (e) {
        showToast('Failed to load profile', 'error');
        console.error(e);
    }
}

// Profile form
document.getElementById('profile-form').addEventListener('submit', async (e) => {
    e.preventDefault();

    const data = {
        display_name: document.getElementById('profile-display-name').value,
        email: document.getElementById('profile-email').value,
    };

    try {
        const res = await fetch('/api/profile', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        const result = await res.json();

        if (result.success) {
            showToast('Profile updated', 'success');
        } else {
            showToast(result.error || 'Update failed', 'error');
        }
    } catch (e) {
        showToast('Failed to update profile', 'error');
    }
});

// Password form
document.getElementById('password-form').addEventListener('submit', async (e) => {
    e.preventDefault();

    const currentPassword = document.getElementById('current-password').value;
    const newPassword = document.getElementById('new-password').value;
    const confirmPassword = document.getElementById('confirm-password').value;

    if (!currentPassword) {
        showToast('Current password required', 'warning');
        return;
    }

    if (!newPassword) {
        showToast('New password required', 'warning');
        return;
    }

    if (newPassword.length < 6) {
        showToast('Password must be at least 6 characters', 'warning');
        return;
    }

    if (newPassword !== confirmPassword) {
        showToast('Passwords do not match', 'warning');
        return;
    }

    try {
        const res = await fetch('/api/profile/password', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                current_password: currentPassword,
                new_password: newPassword
            })
        });
        const result = await res.json();

        if (result.success) {
            showToast('Password changed successfully', 'success');
            document.getElementById('password-form').reset();
        } else {
            showToast(result.error || 'Change failed', 'error');
        }
    } catch (e) {
        showToast('Failed to change password', 'error');
    }
});
