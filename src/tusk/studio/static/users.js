// Tusk Users Management Page JavaScript

let currentTab = 'users';

document.addEventListener('DOMContentLoaded', () => {
    checkAuth();
    loadUsers();
    loadGroups();
});

async function checkAuth() {
    const res = await fetch('/api/auth/status');
    const data = await res.json();

    if (data.auth_enabled && !data.user) {
        window.location.href = '/login?redirect=/users';
        return;
    }

    if (data.user && !data.user.is_admin) {
        showToast('Access denied. Admin privileges required.', 'error');
        window.location.href = '/';
    }
}

window.showTab = function(tab) {
    currentTab = tab;

    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active', 'bg-[#21262d]');
    });
    document.getElementById(`tab-${tab}`).classList.add('active', 'bg-[#21262d]');

    document.getElementById('panel-users').classList.toggle('hidden', tab !== 'users');
    document.getElementById('panel-groups').classList.toggle('hidden', tab !== 'groups');
}

async function loadUsers() {
    const res = await fetch('/api/users/');
    const data = await res.json();

    const tbody = document.getElementById('users-table');

    if (!data.users || data.users.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="px-4 py-8 text-center text-[#8b949e]">No users found</td></tr>';
        return;
    }

    // Fetch group info for each user
    const usersWithGroups = await Promise.all(data.users.map(async u => {
        try {
            const userRes = await fetch(`/api/users/${u.id}`);
            const userData = await userRes.json();
            u.groups = userData.groups || [];
        } catch {
            u.groups = [];
        }
        return u;
    }));

    tbody.innerHTML = usersWithGroups.map(u => `
        <tr class="user-row border-b border-[#30363d]">
            <td class="px-4 py-3">
                <div class="flex items-center gap-3">
                    <div class="w-8 h-8 rounded-full bg-indigo-500/20 flex items-center justify-center text-indigo-400 font-medium">
                        ${(u.display_name || u.username).charAt(0).toUpperCase()}
                    </div>
                    <div>
                        <div class="font-medium">${u.display_name || u.username}</div>
                        <div class="text-xs text-[#8b949e]">@${u.username}</div>
                    </div>
                </div>
            </td>
            <td class="px-4 py-3 text-sm text-[#8b949e]">${u.email || '-'}</td>
            <td class="px-4 py-3">
                ${u.is_admin
                    ? '<span class="px-2 py-1 rounded text-xs bg-purple-500/20 text-purple-400">Admin</span>'
                    : '<span class="px-2 py-1 rounded text-xs bg-[#21262d] text-[#8b949e]">User</span>'}
            </td>
            <td class="px-4 py-3">
                ${u.groups && u.groups.length > 0
                    ? u.groups.map(g => `<span class="px-1.5 py-0.5 rounded text-xs bg-indigo-500/20 text-indigo-400 mr-1">${g.name}</span>`).join('')
                    : '<span class="text-xs text-[#8b949e]">-</span>'}
            </td>
            <td class="px-4 py-3">
                ${u.is_active
                    ? '<span class="px-2 py-1 rounded text-xs bg-green-500/20 text-green-400">Active</span>'
                    : '<span class="px-2 py-1 rounded text-xs bg-red-500/20 text-red-400">Inactive</span>'}
            </td>
            <td class="px-4 py-3 text-sm text-[#8b949e]">
                ${u.last_login ? new Date(u.last_login).toLocaleString() : 'Never'}
            </td>
            <td class="px-4 py-3 text-right">
                <button onclick="editUser('${u.id}')" class="p-1.5 hover:bg-[#21262d] rounded" title="Edit">
                    <i data-lucide="pencil" class="w-4 h-4"></i>
                </button>
                <button onclick="deleteUser('${u.id}', '${u.username}')" class="p-1.5 hover:bg-red-500/20 text-red-400 rounded" title="Delete">
                    <i data-lucide="trash-2" class="w-4 h-4"></i>
                </button>
            </td>
        </tr>
    `).join('');

    lucide.createIcons();
}

async function loadGroups() {
    const res = await fetch('/api/groups/');
    const data = await res.json();

    const grid = document.getElementById('groups-grid');

    if (!data.groups || data.groups.length === 0) {
        grid.innerHTML = '<div class="text-[#8b949e]">No groups found</div>';
        return;
    }

    grid.innerHTML = data.groups.map(g => `
        <div class="bg-[#161b22] border border-[#30363d] rounded-lg p-4">
            <div class="flex items-center justify-between mb-2">
                <h4 class="font-medium">${g.name}</h4>
                <span class="text-xs text-[#8b949e]">${g.permissions.length} permissions</span>
            </div>
            <p class="text-sm text-[#8b949e] mb-3">${g.description || 'No description'}</p>
            <div class="flex flex-wrap gap-1">
                ${g.permissions.slice(0, 3).map(p => `
                    <span class="px-1.5 py-0.5 rounded text-xs bg-[#21262d] text-[#8b949e]">${p.split('.')[0]}</span>
                `).join('')}
                ${g.permissions.length > 3 ? `<span class="px-1.5 py-0.5 rounded text-xs bg-[#21262d] text-[#8b949e]">+${g.permissions.length - 3}</span>` : ''}
            </div>
        </div>
    `).join('');
}

window.showCreateUserModal = function() {
    document.getElementById('create-user-modal').classList.remove('hidden');
    document.getElementById('create-user-form').reset();
}

window.hideCreateUserModal = function() {
    document.getElementById('create-user-modal').classList.add('hidden');
}

window.createUser = async function(event) {
    event.preventDefault();
    const form = event.target;
    const data = {
        username: form.username.value.trim(),
        password: form.password.value,
        display_name: form.display_name.value.trim(),
        email: form.email.value.trim(),
        is_admin: form.is_admin.checked,
    };

    const res = await fetch('/api/users/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
    });

    const result = await res.json();
    if (result.success) {
        hideCreateUserModal();
        loadUsers();
    } else {
        showToast('Error: ' + (result.error || 'Failed to create user'), 'error');
    }
}

let editingUserId = null;
let allGroups = [];
let userGroups = [];

window.editUser = async function(userId) {
    editingUserId = userId;

    // Fetch user details and all groups in parallel
    const [userRes, groupsRes] = await Promise.all([
        fetch(`/api/users/${userId}`),
        fetch('/api/groups/')
    ]);

    const userData = await userRes.json();
    const groupsData = await groupsRes.json();

    if (userData.error) {
        showToast('Error: ' + userData.error, 'error');
        return;
    }

    const u = userData.user;
    document.getElementById('edit-user-id').value = u.id;
    document.getElementById('edit-username').value = u.username;
    document.getElementById('edit-display-name').value = u.display_name || '';
    document.getElementById('edit-email').value = u.email || '';
    document.getElementById('edit-is-admin').checked = u.is_admin;
    document.getElementById('edit-is-active').checked = u.is_active;

    // Store groups for later
    allGroups = groupsData.groups || [];
    userGroups = userData.groups || [];
    const userGroupIds = userGroups.map(g => g.id);

    // Render group checkboxes
    const groupsContainer = document.getElementById('edit-user-groups');
    if (allGroups.length === 0) {
        groupsContainer.innerHTML = '<div class="text-[#8b949e] text-sm">No groups available</div>';
    } else {
        groupsContainer.innerHTML = allGroups.map(g => `
            <label class="flex items-center gap-2 p-1.5 rounded hover:bg-[#21262d] cursor-pointer">
                <input type="checkbox" name="group_${g.id}" value="${g.id}"
                    ${userGroupIds.includes(g.id) ? 'checked' : ''}
                    class="rounded accent-indigo-500">
                <span class="text-sm">${g.name}</span>
            </label>
        `).join('');
    }

    document.getElementById('edit-user-modal').classList.remove('hidden');
}

window.hideEditUserModal = function() {
    document.getElementById('edit-user-modal').classList.add('hidden');
    editingUserId = null;
}

window.updateUser = async function(event) {
    event.preventDefault();

    const data = {
        display_name: document.getElementById('edit-display-name').value.trim(),
        email: document.getElementById('edit-email').value.trim(),
        is_admin: document.getElementById('edit-is-admin').checked,
        is_active: document.getElementById('edit-is-active').checked,
    };

    // Update user profile
    const res = await fetch(`/api/users/${editingUserId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
    });

    const result = await res.json();
    if (!result.success) {
        showToast('Error: ' + (result.error || 'Failed to update user'), 'error');
        return;
    }

    // Update group memberships
    const currentGroupIds = userGroups.map(g => g.id);
    const selectedGroupIds = [];

    // Get selected groups from checkboxes
    allGroups.forEach(g => {
        const checkbox = document.querySelector(`input[name="group_${g.id}"]`);
        if (checkbox && checkbox.checked) {
            selectedGroupIds.push(g.id);
        }
    });

    // Groups to add
    const toAdd = selectedGroupIds.filter(id => !currentGroupIds.includes(id));
    // Groups to remove
    const toRemove = currentGroupIds.filter(id => !selectedGroupIds.includes(id));

    // Process group changes
    for (const groupId of toAdd) {
        await fetch(`/api/users/${editingUserId}/groups`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ group_id: groupId }),
        });
    }

    for (const groupId of toRemove) {
        await fetch(`/api/users/${editingUserId}/groups/${groupId}/remove`, {
            method: 'POST',
        });
    }

    hideEditUserModal();
    showToast('User updated successfully', 'success');
    loadUsers();
}

window.showResetPasswordModal = function() {
    document.getElementById('reset-password-modal').classList.remove('hidden');
    document.getElementById('new-password').value = '';
}

window.hideResetPasswordModal = function() {
    document.getElementById('reset-password-modal').classList.add('hidden');
}

window.resetPassword = async function(event) {
    event.preventDefault();
    const password = document.getElementById('new-password').value;

    const res = await fetch(`/api/users/${editingUserId}/password`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password }),
    });

    const result = await res.json();
    if (result.success) {
        hideResetPasswordModal();
        showToast('Password reset successfully', 'success');
    } else {
        showToast('Error: ' + (result.error || 'Failed to reset password'), 'error');
    }
}

window.deleteUser = async function(userId, username) {
    if (!confirm(`Delete user "${username}"? This cannot be undone.`)) return;

    const res = await fetch(`/api/users/${userId}/delete`, { method: 'POST' });
    const result = await res.json();

    if (result.success) {
        loadUsers();
    } else {
        showToast('Error: ' + (result.error || 'Failed to delete user'), 'error');
    }
}
