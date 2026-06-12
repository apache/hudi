/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

(function () {
  'use strict';

  // DOM references
  var stateEmpty = document.getElementById('stateEmpty');
  var stateLoading = document.getElementById('stateLoading');
  var stateError = document.getElementById('stateError');
  var stateLoaded = document.getElementById('stateLoaded');
  var errorMessage = document.getElementById('errorMessage');
  var instantCount = document.getElementById('instantCount');
  var detailCard = document.getElementById('detailCard');
  var detailInstantId = document.getElementById('detailInstantId');
  var detailAction = document.getElementById('detailAction');
  var detailState = document.getElementById('detailState');
  var detailMeta = document.getElementById('detailMeta');
  var detailBody = document.getElementById('detailBody');

  var timeline = null;
  var cleanRangeItemId = 'clean-range-bg';
  var allItems = null;
  var filteredView = null;
  var activeStates = new Set(['COMPLETED', 'INFLIGHT', 'REQUESTED']);
  var allLoadedActions = new Set();
  var activeActions = new Set();
  var currentTablePath = null;

  // Lazy-loaded data caches for tabs
  var tableConfigData = null;
  var schemaHistoryData = null;

  // Maps each Hudi action type to a group row.
  // Matches HoodieTimeline.VALID_ACTIONS_IN_TIMELINE.
  var actionToGroupId = {
    'commit': 0,
    'deltacommit': 1,
    'clean': 2,
    'savepoint': 3,
    'restore': 4,
    'rollback': 5,
    'compaction': 6,
    'logcompaction': 7,
    'replacecommit': 8,
    'clustering': 9,
    'indexing': 10
  };

  var groups = Object.keys(actionToGroupId).map(function (action) {
    return { id: actionToGroupId[action], content: action };
  });

  // State management
  var STATES = {EMPTY: 'EMPTY', LOADING: 'LOADING', ERROR: 'ERROR', LOADED: 'LOADED'};
  var stateElements = {
    EMPTY: stateEmpty,
    LOADING: stateLoading,
    ERROR: stateError,
    LOADED: stateLoaded
  };

  function setState(state, data) {
    Object.keys(stateElements).forEach(function (key) {
      stateElements[key].classList.add('d-none');
    });
    stateElements[state].classList.remove('d-none');

    if (state === STATES.ERROR && data) {
      errorMessage.textContent = data;
    }

    // Hide detail card when switching states
    if (state !== STATES.LOADED) {
      detailCard.classList.add('d-none');
    }
  }

  // State badge color mapping
  var stateBadgeClass = {
    'COMPLETED': 'bg-success',
    'INFLIGHT': 'bg-warning text-dark',
    'REQUESTED': 'bg-danger'
  };

  function displayInstantDetails(item) {
    var html = '<p>Start: ' + item.start + '</p>';
    if (item.end !== undefined) {
      html += '<p>End: ' + item.end + '</p>';
      var duration = localize(timeDiff(item.end, item.start));
      html += '<p>Duration: ' + duration + '</p>';
    }
    html += '<p>Instant: ' + item.content + '</p>';
    return html;
  }

  var options = {
    width: '100%',
    height: '100%',
    margin: { item: 10, axis: 5 },
    horizontalScroll: true,
    zoomKey: 'shiftKey',
    editable: false,
    tooltip: {
      delay: 0,
      template: displayInstantDetails
    }
  };

  // Navigation: Go to Now
  document.getElementById('goToNowBtn').addEventListener('click', function () {
    if (timeline) {
      timeline.moveTo(new Date());
    }
  });

  // Navigation: Instant search
  document.getElementById('instantSearchBtn').addEventListener('click', function () {
    focusOnInstant();
  });
  document.getElementById('instantSearchInput').addEventListener('keydown', function (e) {
    if (e.key === 'Enter') {
      e.preventDefault();
      focusOnInstant();
    }
  });

  function focusOnInstant() {
    if (!timeline || !allItems) return;
    var query = document.getElementById('instantSearchInput').value.trim();
    if (!query) return;

    var searchItems = allItems.get();
    var match = null;
    for (var i = 0; i < searchItems.length; i++) {
      if (searchItems[i].requestTs === query || searchItems[i].completionTs === query) {
        match = searchItems[i];
        break;
      }
    }

    if (match) {
      timeline.focus(match.id, { animation: { duration: 500, easingFunction: 'easeInOutQuad' } });
      timeline.setSelection([match.id]);
    } else {
      var parsed = parseHudiTimestamp(query);
      if (parsed && parsed !== query) {
        timeline.moveTo(new Date(parsed), { animation: { duration: 500, easingFunction: 'easeInOutQuad' } });
      } else {
        var input = document.getElementById('instantSearchInput');
        input.classList.add('is-invalid');
        setTimeout(function () { input.classList.remove('is-invalid'); }, 1500);
      }
    }
  }

  // Summary statistics computation
  function computeStats() {
    if (!filteredView) return;

    var items = filteredView.get({
      filter: function (item) { return item.type !== 'background'; }
    });

    var total = items.length;
    document.getElementById('statTotal').textContent = total;

    var completed = 0, inflight = 0, requested = 0;
    items.forEach(function (item) {
      if (item.state === 'COMPLETED') completed++;
      else if (item.state === 'INFLIGHT') inflight++;
      else if (item.state === 'REQUESTED') requested++;
    });

    var badges = document.getElementById('statByState').children;
    badges[0].textContent = completed;
    badges[1].textContent = inflight;
    badges[2].textContent = requested;

    // Time span
    if (items.length > 0) {
      var dates = items.map(function (i) { return new Date(i.start); }).sort(function (a, b) { return a - b; });
      var spanMs = dates[dates.length - 1] - dates[0];
      document.getElementById('statTimeSpan').textContent = localize({
        days: Math.floor(spanMs / (1000 * 60 * 60 * 24)),
        hours: Math.floor(spanMs / (1000 * 60 * 60)) % 24,
        minutes: Math.floor(spanMs / (1000 * 60)) % 60,
        seconds: Math.floor(spanMs / 1000) % 60
      });
    } else {
      document.getElementById('statTimeSpan').textContent = '\u2014';
    }

    // Avg commit duration
    var commitDurations = [];
    items.forEach(function (item) {
      if ((item.action === 'commit' || item.action === 'deltacommit')
          && item.state === 'COMPLETED' && item.end) {
        var diff = new Date(item.end) - new Date(item.start);
        if (diff > 0) commitDurations.push(diff);
      }
    });

    if (commitDurations.length > 0) {
      var avg = commitDurations.reduce(function (a, b) { return a + b; }, 0) / commitDurations.length;
      document.getElementById('statAvgDuration').textContent = localize({
        days: Math.floor(avg / (1000 * 60 * 60 * 24)),
        hours: Math.floor(avg / (1000 * 60 * 60)) % 24,
        minutes: Math.floor(avg / (1000 * 60)) % 60,
        seconds: Math.floor(avg / 1000) % 60
      });
    } else {
      document.getElementById('statAvgDuration').textContent = '\u2014';
    }

    // Update instant count badge
    instantCount.textContent = total + ' instants';
  }

  // Build action filter pills dynamically based on loaded data
  function buildActionFilters() {
    var container = document.getElementById('actionFilters');
    container.innerHTML = '';
    Object.keys(actionToGroupId).forEach(function (action) {
      if (!allLoadedActions.has(action)) return;
      var btn = document.createElement('button');
      btn.className = 'btn btn-sm filter-pill active';
      btn.setAttribute('data-filter-action', action);
      btn.textContent = action;
      container.appendChild(btn);
    });
  }

  // Reset state filter pill UI to all-active
  function resetFilterPillUI() {
    document.querySelectorAll('[data-filter-state]').forEach(function (btn) {
      btn.classList.add('active');
    });
  }

  // Filter pill click handler (delegated)
  document.getElementById('filterControls').addEventListener('click', function (e) {
    var btn = e.target.closest('.filter-pill');
    if (!btn || !filteredView) return;

    var stateFilter = btn.getAttribute('data-filter-state');
    var actionFilter = btn.getAttribute('data-filter-action');

    if (stateFilter) {
      if (activeStates.has(stateFilter)) {
        activeStates.delete(stateFilter);
        btn.classList.remove('active');
      } else {
        activeStates.add(stateFilter);
        btn.classList.add('active');
      }
    }

    if (actionFilter) {
      if (activeActions.has(actionFilter)) {
        activeActions.delete(actionFilter);
        btn.classList.remove('active');
      } else {
        activeActions.add(actionFilter);
        btn.classList.add('active');
      }
    }

    filteredView.refresh();
    computeStats();
  });

  document.getElementById('timelineForm').addEventListener('submit', function (e) {
    e.preventDefault();

    setState(STATES.LOADING);

    var tablePath = document.getElementById('tablePath').value;
    var timelineContainer = document.getElementById('timeline');

    // Reset cached tab data on new table load
    tableConfigData = null;
    schemaHistoryData = null;

    // Switch to Timeline tab
    var timelineTab = document.getElementById('tab-timeline');
    if (timelineTab) {
      bootstrap.Tab.getOrCreateInstance(timelineTab).show();
    }

    fetch('/v2/hoodie/view/timeline/instants/all?basepath=' + encodeURIComponent(tablePath))
      .then(function (res) {
        if (!res.ok) {
          throw new Error('HTTP ' + res.status + ': ' + res.statusText);
        }
        return res.json();
      })
      .then(function (data) {
        var instants = data.instants;
        if (!instants || instants.length === 0) {
          setState(STATES.ERROR, 'No instants found for this table.');
          return;
        }

        setState(STATES.LOADED);

        var items = instants.map(function (instant, index) {
          var requestTs = instant.requestTs;
          var completionTs = instant.completionTs;
          var action = instant.action;
          var state = instant.state;
          var groupId = actionToGroupId[action] !== undefined ? actionToGroupId[action] : -1;

          var effectiveRequestTs = (requestTs === '00000000000000000') ? completionTs : requestTs;
          var requestTsFormatted = parseHudiTimestamp(effectiveRequestTs);
          var completionTsFormatted = completionTs ? parseHudiTimestamp(completionTs) : null;

          var item = {
            id: index + 1,
            content: requestTs + '__' + action + '__' + state,
            start: requestTsFormatted,
            group: groupId,
            className: 'state-' + state,
            requestTs: requestTs,
            completionTs: completionTs,
            action: action,
            state: state
          };

          // Completed instants with a completion time render as range bars
          if (completionTsFormatted && state === 'COMPLETED') {
            item.end = completionTsFormatted;
          }

          return item;
        });

        // Set up DataSet, DataView, and filters
        allItems = new vis.DataSet(items);
        allLoadedActions = new Set(items.map(function (i) { return i.action; }));
        activeActions = new Set(allLoadedActions);
        activeStates = new Set(['COMPLETED', 'INFLIGHT', 'REQUESTED']);
        resetFilterPillUI();
        buildActionFilters();

        filteredView = new vis.DataView(allItems, {
          filter: function (item) {
            if (item.type === 'background') return true;
            return activeStates.has(item.state) && activeActions.has(item.action);
          }
        });

        if (timeline === null) {
          timeline = new vis.Timeline(timelineContainer, filteredView, groups, options);
        } else {
          timeline.setItems(filteredView);
        }

        computeStats();
        currentTablePath = tablePath;

        // Update URL with table path (preserve tab param)
        updateUrlState();

        timeline.off('select');
        timeline.on('select', function (props) {
          onSelect(props, currentTablePath);
        });
      })
      .catch(function (err) {
        setState(STATES.ERROR, 'Failed to load timeline: ' + err.message);
        console.error(err);
      });
  });

  function getCleanPolicy(cleanMetadataJson) {
    var pm = cleanMetadataJson.partitionMetadata;
    if (!pm) return null;
    var keys = Object.keys(pm);
    if (keys.length === 0) return null;
    return pm[keys[0]].policy || null;
  }

  function findPreviousCompletedClean(currentRequestTs) {
    var cleans = allItems.get({
      filter: function (item) {
        return item.action === 'clean' && item.state === 'COMPLETED' && item.requestTs < currentRequestTs;
      }
    });
    if (cleans.length === 0) return null;
    cleans.sort(function (a, b) { return a.requestTs < b.requestTs ? 1 : -1; });
    return cleans[0];
  }

  function onSelect(props, tablePath) {
    allItems.remove(cleanRangeItemId);

    if (props.items.length === 0) {
      detailCard.classList.add('d-none');
      return;
    }

    var item = allItems.get(props.items[0]);

    // Populate detail header
    detailInstantId.textContent = item.requestTs;
    detailAction.textContent = item.action;
    detailState.textContent = item.state;
    detailState.className = 'badge ' + (stateBadgeClass[item.state] || 'bg-secondary');

    // Build metadata line
    var metaParts = [];
    var requestTsFormatted = parseHudiTimestamp(item.requestTs);
    if (requestTsFormatted) {
      metaParts.push('Request: ' + requestTsFormatted);
    }
    if (item.end) {
      metaParts.push('Completed: ' + item.end);
      var duration = localize(timeDiff(item.end, item.start));
      metaParts.push('Duration: ' + duration);
    }
    detailMeta.textContent = metaParts.join('  |  ');

    // Show detail card with loading state
    detailCard.classList.remove('d-none');
    detailBody.innerHTML = '';
    var loadingEl = document.createElement('div');
    loadingEl.className = 'text-center text-muted py-3';
    loadingEl.innerHTML = '<div class="spinner-border spinner-border-sm me-2" role="status"></div>Loading instant details...';
    detailBody.appendChild(loadingEl);

    var url = '/v2/hoodie/view/timeline/instant?basepath=' + encodeURIComponent(tablePath)
      + '&instant=' + item.requestTs
      + '&instantaction=' + item.action
      + '&instantstate=' + item.state;

    fetch(url)
      .then(function (res) {
        if (!res.ok) {
          throw new Error('HTTP ' + res.status);
        }
        return res.json();
      })
      .then(function (json) {
        detailBody.innerHTML = '';
        renderjson.set_show_to_level(1);
        renderjson.set_icons('\u25B6', '\u25BC');
        renderjson.set_sort_objects(true);
        detailBody.appendChild(renderjson(json));

        // Show clean range only for KEEP_LATEST_COMMITS policy.
        // The range spans from the previous clean's earliestCommitToRetain
        // to this clean's earliestCommitToRetain, matching the incremental
        // cleaning scan window in CleanPlanner.
        var cleanPolicy = getCleanPolicy(json);
        if (cleanPolicy === 'KEEP_LATEST_COMMITS' && json.earliestCommitToRetain) {
          var prevClean = findPreviousCompletedClean(item.requestTs);
          if (prevClean) {
            var prevUrl = '/v2/hoodie/view/timeline/instant?basepath=' + encodeURIComponent(tablePath)
              + '&instant=' + prevClean.requestTs
              + '&instantaction=' + prevClean.action
              + '&instantstate=' + prevClean.state;
            fetch(prevUrl)
              .then(function (res) { return res.ok ? res.json() : null; })
              .then(function (prevJson) {
                if (prevJson && prevJson.earliestCommitToRetain) {
                  var rangeStart = parseHudiTimestamp(prevJson.earliestCommitToRetain);
                  var rangeEnd = parseHudiTimestamp(json.earliestCommitToRetain);
                  allItems.add({
                    id: cleanRangeItemId,
                    type: 'background',
                    content: 'Clean Range',
                    start: rangeStart,
                    end: rangeEnd,
                    className: 'clean-range-bg'
                  });
                  detailMeta.textContent += '  |  Clean range: ' + rangeStart + ' \u2192 ' + rangeEnd;
                }
              })
              .catch(function (err) { console.error('Failed to fetch previous clean metadata:', err); });
          }
        }
      })
      .catch(function (err) {
        console.error(err);
        detailBody.innerHTML = '';
        var alertEl = document.createElement('div');
        alertEl.className = 'alert alert-danger mb-0';
        alertEl.textContent = 'Failed to fetch instant details: ' + err.message;
        detailBody.appendChild(alertEl);
      });
  }

  // ===== Tab handling =====

  // Tab shown event: lazy-load data
  var mainTabsEl = document.getElementById('mainTabs');
  mainTabsEl.addEventListener('shown.bs.tab', function (e) {
    var targetId = e.target.getAttribute('data-bs-target');
    updateUrlState();

    if (targetId === '#tabConfig') {
      loadTableConfig();
    } else if (targetId === '#tabSchema') {
      loadSchemaHistory();
    }
  });

  function loadTableConfig() {
    if (!currentTablePath) return;
    if (tableConfigData) {
      renderTableConfig(tableConfigData);
      return;
    }

    document.getElementById('configLoading').classList.remove('d-none');
    document.getElementById('configContent').classList.add('d-none');
    document.getElementById('configError').classList.add('d-none');

    fetch('/v2/hoodie/view/table/config?basepath=' + encodeURIComponent(currentTablePath))
      .then(function (res) {
        if (!res.ok) throw new Error('HTTP ' + res.status + ': ' + res.statusText);
        return res.json();
      })
      .then(function (data) {
        tableConfigData = data;
        document.getElementById('configLoading').classList.add('d-none');
        renderTableConfig(data);
      })
      .catch(function (err) {
        document.getElementById('configLoading').classList.add('d-none');
        document.getElementById('configError').classList.remove('d-none');
        document.getElementById('configErrorMessage').textContent = 'Failed to load table config: ' + err.message;
      });
  }

  function renderTableConfig(data) {
    var tbody = document.getElementById('configTableBody');
    tbody.innerHTML = '';
    var props = data.properties || {};
    Object.keys(props).forEach(function (key) {
      var tr = document.createElement('tr');
      var tdKey = document.createElement('td');
      tdKey.textContent = key;
      var tdVal = document.createElement('td');
      tdVal.textContent = props[key];
      tr.appendChild(tdKey);
      tr.appendChild(tdVal);
      tbody.appendChild(tr);
    });
    document.getElementById('configContent').classList.remove('d-none');
  }

  // Config filter
  document.getElementById('configFilter').addEventListener('input', function () {
    var query = this.value.toLowerCase();
    var rows = document.getElementById('configTableBody').querySelectorAll('tr');
    rows.forEach(function (row) {
      var text = row.textContent.toLowerCase();
      row.classList.toggle('d-none', query && text.indexOf(query) === -1);
    });
  });

  function loadSchemaHistory() {
    if (!currentTablePath) return;
    if (schemaHistoryData) {
      renderSchemaHistory(schemaHistoryData);
      return;
    }

    document.getElementById('schemaLoading').classList.remove('d-none');
    document.getElementById('schemaContent').classList.add('d-none');
    document.getElementById('schemaError').classList.add('d-none');

    fetch('/v2/hoodie/view/table/schema/history?basepath=' + encodeURIComponent(currentTablePath))
      .then(function (res) {
        if (!res.ok) throw new Error('HTTP ' + res.status + ': ' + res.statusText);
        return res.json();
      })
      .then(function (data) {
        schemaHistoryData = data;
        document.getElementById('schemaLoading').classList.add('d-none');
        renderSchemaHistory(data);
      })
      .catch(function (err) {
        document.getElementById('schemaLoading').classList.add('d-none');
        document.getElementById('schemaError').classList.remove('d-none');
        document.getElementById('schemaErrorMessage').textContent = 'Failed to load schema history: ' + err.message;
      });
  }

  function renderSchemaHistory(data) {
    // Current schema tree
    var treeContainer = document.getElementById('currentSchemaTree');
    treeContainer.innerHTML = '';
    if (data.currentSchema) {
      try {
        var schemaObj = JSON.parse(data.currentSchema);
        renderjson.set_show_to_level(2);
        renderjson.set_icons('\u25B6', '\u25BC');
        renderjson.set_sort_objects(false);
        treeContainer.appendChild(renderjson(schemaObj));

        // Fields summary table
        var fieldsBody = document.getElementById('currentFieldsBody');
        fieldsBody.innerHTML = '';
        var fields = schemaObj.fields || [];
        fields.forEach(function (field) {
          var tr = document.createElement('tr');
          var tdName = document.createElement('td');
          tdName.className = 'font-monospace';
          tdName.textContent = field.name;
          var tdType = document.createElement('td');
          tdType.textContent = formatAvroType(field.type);
          var tdNullable = document.createElement('td');
          tdNullable.textContent = isNullable(field.type) ? 'Yes' : 'No';
          tr.appendChild(tdName);
          tr.appendChild(tdType);
          tr.appendChild(tdNullable);
          fieldsBody.appendChild(tr);
        });
      } catch (e) {
        treeContainer.textContent = data.currentSchema;
      }
    } else {
      treeContainer.innerHTML = '<p class="text-muted">No schema available</p>';
      document.getElementById('currentFieldsBody').innerHTML = '';
    }

    // Schema change history
    var historyList = document.getElementById('schemaHistoryList');
    historyList.innerHTML = '';

    // Prefer internal schema history from .schema directory (richer evolution tracking)
    if (data.internalSchemaHistory) {
      try {
        var internalData = JSON.parse(data.internalSchemaHistory);
        var schemas = (internalData.schemas || []).slice();
        // Sort by version_id ascending (oldest first)
        schemas.sort(function (a, b) { return a.version_id - b.version_id; });

        if (schemas.length === 0) {
          historyList.innerHTML = '<div class="alert alert-info">No schema changes found</div>';
          document.getElementById('schemaContent').classList.remove('d-none');
          return;
        }

        // Build diff cards in reverse chronological order (newest first)
        for (var i = schemas.length - 1; i >= 0; i--) {
          var schema = schemas[i];
          var card = document.createElement('div');
          card.className = 'card mb-2';

          var header = document.createElement('div');
          header.className = 'card-header d-flex align-items-center gap-2';

          var tsCode = document.createElement('code');
          tsCode.textContent = schema.version_id === 0 ? 'Initial' : String(schema.version_id);

          header.appendChild(tsCode);

          if (i > 0) {
            var diff = diffInternalSchemas(schemas[i - 1], schema);
            var summaryParts = [];
            if (diff.added.length > 0) summaryParts.push(diff.added.length + ' added');
            if (diff.removed.length > 0) summaryParts.push(diff.removed.length + ' removed');
            if (diff.changed.length > 0) summaryParts.push(diff.changed.length + ' changed');

            if (summaryParts.length > 0) {
              var summarySpan = document.createElement('span');
              summarySpan.className = 'text-muted small ms-auto';
              summarySpan.textContent = summaryParts.join(', ');
              header.appendChild(summarySpan);
            }

            var collapseId = 'schemaDiff' + i;
            var toggleBtn = document.createElement('button');
            toggleBtn.className = 'btn btn-sm btn-outline-secondary ms-2';
            toggleBtn.setAttribute('data-bs-toggle', 'collapse');
            toggleBtn.setAttribute('data-bs-target', '#' + collapseId);
            toggleBtn.textContent = 'Details';
            header.appendChild(toggleBtn);

            var collapseDiv = document.createElement('div');
            collapseDiv.className = 'collapse';
            collapseDiv.id = collapseId;
            var collapseBody = document.createElement('div');
            collapseBody.className = 'card-body';
            collapseBody.appendChild(renderDiffDetail(diff));
            collapseDiv.appendChild(collapseBody);

            card.appendChild(header);
            card.appendChild(collapseDiv);
          } else {
            var initialBadge = document.createElement('span');
            initialBadge.className = 'badge bg-secondary ms-auto';
            initialBadge.textContent = 'Initial schema';
            header.appendChild(initialBadge);
            card.appendChild(header);
          }

          historyList.appendChild(card);
        }

        document.getElementById('schemaContent').classList.remove('d-none');
        return;
      } catch (e) {
        // Fall through to commit metadata history
      }
    }

    // Fallback: use commit metadata history
    var history = data.history || [];

    if (history.length === 0) {
      historyList.innerHTML = '<div class="alert alert-info">No schema changes found</div>';
      document.getElementById('schemaContent').classList.remove('d-none');
      return;
    }

    // Build diff cards in reverse chronological order
    for (var i = history.length - 1; i >= 0; i--) {
      var entry = history[i];
      var card = document.createElement('div');
      card.className = 'card mb-2';

      var header = document.createElement('div');
      header.className = 'card-header d-flex align-items-center gap-2';

      var tsCode = document.createElement('code');
      tsCode.textContent = entry.instant;

      var actionBadge = document.createElement('span');
      actionBadge.className = 'badge bg-primary';
      actionBadge.textContent = entry.action;

      header.appendChild(tsCode);
      header.appendChild(actionBadge);

      // Compute diff with previous schema if available
      if (i > 0) {
        var diff = diffSchemas(history[i - 1].schema, entry.schema);
        var summaryParts = [];
        if (diff.added.length > 0) summaryParts.push(diff.added.length + ' added');
        if (diff.removed.length > 0) summaryParts.push(diff.removed.length + ' removed');
        if (diff.changed.length > 0) summaryParts.push(diff.changed.length + ' changed');

        if (summaryParts.length > 0) {
          var summarySpan = document.createElement('span');
          summarySpan.className = 'text-muted small ms-auto';
          summarySpan.textContent = summaryParts.join(', ');
          header.appendChild(summarySpan);
        }

        // Collapsible diff detail
        var collapseId = 'schemaDiff' + i;
        var toggleBtn = document.createElement('button');
        toggleBtn.className = 'btn btn-sm btn-outline-secondary ms-2';
        toggleBtn.setAttribute('data-bs-toggle', 'collapse');
        toggleBtn.setAttribute('data-bs-target', '#' + collapseId);
        toggleBtn.textContent = 'Details';
        header.appendChild(toggleBtn);

        var collapseDiv = document.createElement('div');
        collapseDiv.className = 'collapse';
        collapseDiv.id = collapseId;
        var collapseBody = document.createElement('div');
        collapseBody.className = 'card-body';
        collapseBody.appendChild(renderDiffDetail(diff));
        collapseDiv.appendChild(collapseBody);

        card.appendChild(header);
        card.appendChild(collapseDiv);
      } else {
        var initialBadge = document.createElement('span');
        initialBadge.className = 'badge bg-secondary ms-auto';
        initialBadge.textContent = 'Initial schema';
        header.appendChild(initialBadge);
        card.appendChild(header);
      }

      historyList.appendChild(card);
    }

    document.getElementById('schemaContent').classList.remove('d-none');
  }

  function formatAvroType(type) {
    if (typeof type === 'string') return type;
    if (Array.isArray(type)) {
      return type.map(formatAvroType).join(' | ');
    }
    if (type && type.type) {
      if (type.type === 'array') return 'array<' + formatAvroType(type.items) + '>';
      if (type.type === 'map') return 'map<' + formatAvroType(type.values) + '>';
      return type.type;
    }
    return JSON.stringify(type);
  }

  function isNullable(type) {
    if (Array.isArray(type)) {
      return type.indexOf('null') !== -1;
    }
    return type === 'null';
  }

  function diffSchemas(olderSchemaStr, newerSchemaStr) {
    var result = { added: [], removed: [], changed: [] };
    try {
      var older = JSON.parse(olderSchemaStr);
      var newer = JSON.parse(newerSchemaStr);
      var olderFields = (older.fields || []);
      var newerFields = (newer.fields || []);

      var olderMap = {};
      olderFields.forEach(function (f) { olderMap[f.name] = f; });
      var newerMap = {};
      newerFields.forEach(function (f) { newerMap[f.name] = f; });

      // Added fields
      newerFields.forEach(function (f) {
        if (!olderMap[f.name]) {
          result.added.push(f);
        }
      });

      // Removed fields
      olderFields.forEach(function (f) {
        if (!newerMap[f.name]) {
          result.removed.push(f);
        }
      });

      // Changed fields
      newerFields.forEach(function (f) {
        if (olderMap[f.name]) {
          var oldType = JSON.stringify(olderMap[f.name].type);
          var newType = JSON.stringify(f.type);
          if (oldType !== newType) {
            result.changed.push({
              name: f.name,
              oldType: formatAvroType(olderMap[f.name].type),
              newType: formatAvroType(f.type)
            });
          }
        }
      });
    } catch (e) {
      // If schemas can't be parsed, return empty diff
    }
    return result;
  }

  function diffInternalSchemas(olderSchema, newerSchema) {
    var result = { added: [], removed: [], changed: [] };
    var olderFields = olderSchema.fields || [];
    var newerFields = newerSchema.fields || [];

    var olderMap = {};
    olderFields.forEach(function (f) { olderMap[f.name] = f; });
    var newerMap = {};
    newerFields.forEach(function (f) { newerMap[f.name] = f; });

    newerFields.forEach(function (f) {
      if (!olderMap[f.name]) {
        result.added.push({ name: f.name, type: f.type });
      }
    });

    olderFields.forEach(function (f) {
      if (!newerMap[f.name]) {
        result.removed.push({ name: f.name, type: f.type });
      }
    });

    newerFields.forEach(function (f) {
      if (olderMap[f.name] && olderMap[f.name].type !== f.type) {
        result.changed.push({
          name: f.name,
          oldType: String(olderMap[f.name].type),
          newType: String(f.type)
        });
      }
    });

    return result;
  }

  function renderDiffDetail(diff) {
    var container = document.createElement('div');

    if (diff.added.length > 0) {
      diff.added.forEach(function (f) {
        var div = document.createElement('div');
        div.className = 'schema-diff-added mb-1 py-1';
        div.innerHTML = '<strong>+ ' + escapeHtml(f.name) + '</strong>: ' + escapeHtml(formatAvroType(f.type));
        container.appendChild(div);
      });
    }

    if (diff.removed.length > 0) {
      diff.removed.forEach(function (f) {
        var div = document.createElement('div');
        div.className = 'schema-diff-removed mb-1 py-1';
        div.innerHTML = '<strong>- ' + escapeHtml(f.name) + '</strong>: ' + escapeHtml(formatAvroType(f.type));
        container.appendChild(div);
      });
    }

    if (diff.changed.length > 0) {
      diff.changed.forEach(function (c) {
        var div = document.createElement('div');
        div.className = 'schema-diff-changed mb-1 py-1';
        div.innerHTML = '<strong>~ ' + escapeHtml(c.name) + '</strong>: '
          + escapeHtml(c.oldType) + ' &rarr; ' + escapeHtml(c.newType);
        container.appendChild(div);
      });
    }

    if (diff.added.length === 0 && diff.removed.length === 0 && diff.changed.length === 0) {
      container.innerHTML = '<span class="text-muted">No field-level changes detected</span>';
    }

    return container;
  }

  function escapeHtml(str) {
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(str));
    return div.innerHTML;
  }

  // ===== URL state management =====

  function getActiveTabKey() {
    var activeTab = mainTabsEl.querySelector('.nav-link.active');
    if (!activeTab) return null;
    var target = activeTab.getAttribute('data-bs-target');
    if (target === '#tabConfig') return 'config';
    if (target === '#tabSchema') return 'schema';
    return null; // timeline is default, no param needed
  }

  function updateUrlState() {
    if (!currentTablePath) return;
    var url = '?path=' + encodeURIComponent(currentTablePath);
    var tabKey = getActiveTabKey();
    if (tabKey) {
      url += '&tab=' + tabKey;
    }
    history.replaceState(null, '', url);
  }

  // ===== Keyboard shortcuts =====

  // Help modal toggle via '?' key
  document.addEventListener('keydown', function (e) {
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
    if (e.key === '?') {
      var modalEl = document.getElementById('helpModal');
      var modal = bootstrap.Modal.getOrCreateInstance(modalEl);
      modal.toggle();
    }
  });

  // Tab switching via 1/2/3 keys and arrow navigation
  document.addEventListener('keydown', function (e) {
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

    // Tab switching
    if (e.key === '1' || e.key === '2' || e.key === '3') {
      if (!stateLoaded.classList.contains('d-none')) {
        var tabIds = ['tab-timeline', 'tab-config', 'tab-schema'];
        var idx = parseInt(e.key, 10) - 1;
        var tabEl = document.getElementById(tabIds[idx]);
        if (tabEl) {
          bootstrap.Tab.getOrCreateInstance(tabEl).show();
        }
      }
      return;
    }

    if (!timeline || !filteredView) return;

    if (e.key === 'Escape') {
      timeline.setSelection([]);
      detailCard.classList.add('d-none');
      allItems.remove(cleanRangeItemId);
      return;
    }

    if (e.key !== 'ArrowRight' && e.key !== 'ArrowLeft') return;
    e.preventDefault();

    var visibleItems = filteredView.get({
      filter: function (item) { return item.type !== 'background'; }
    });
    visibleItems.sort(function (a, b) { return new Date(a.start) - new Date(b.start); });

    if (visibleItems.length === 0) return;

    var selected = timeline.getSelection();
    var currentIndex = -1;
    if (selected.length > 0) {
      for (var i = 0; i < visibleItems.length; i++) {
        if (visibleItems[i].id === selected[0]) {
          currentIndex = i;
          break;
        }
      }
    }

    var nextIndex;
    if (e.key === 'ArrowRight') {
      nextIndex = currentIndex < visibleItems.length - 1 ? currentIndex + 1 : 0;
    } else {
      nextIndex = currentIndex > 0 ? currentIndex - 1 : visibleItems.length - 1;
    }

    var nextItem = visibleItems[nextIndex];
    timeline.setSelection([nextItem.id]);
    timeline.focus(nextItem.id, { animation: { duration: 300, easingFunction: 'easeInOutQuad' } });

    // Trigger detail panel update
    if (currentTablePath) {
      onSelect({ items: [nextItem.id] }, currentTablePath);
    }
  });

  // Parses a Hudi timestamp (yyyyMMddHHmmssSSS) to a readable date string.
  function parseHudiTimestamp(ts) {
    if (!ts) return ts;
    var pattern = /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d{3})$/;
    var match = ts.match(pattern);
    if (!match) return ts;
    return match[1] + '-' + match[2] + '-' + match[3]
      + ' ' + match[4] + ':' + match[5] + ':' + match[6];
  }

  // Computes the time difference between two date strings.
  function timeDiff(endStr, startStr) {
    var start = new Date(startStr);
    var end = new Date(endStr);
    var diffMs = end - start;

    var seconds = Math.floor(diffMs / 1000) % 60;
    var minutes = Math.floor(diffMs / (1000 * 60)) % 60;
    var hours = Math.floor(diffMs / (1000 * 60 * 60)) % 24;
    var days = Math.floor(diffMs / (1000 * 60 * 60 * 24));

    return { days: days, hours: hours, minutes: minutes, seconds: seconds };
  }

  // Formats a time difference object to a human-readable string.
  function localize(diff) {
    var parts = [];
    if (diff.days > 0) parts.push(diff.days + 'd');
    if (diff.hours > 0) parts.push(diff.hours + 'h');
    if (diff.minutes > 0) parts.push(diff.minutes + 'm');
    if (diff.seconds > 0 || parts.length === 0) parts.push(diff.seconds + 's');
    return parts.join(' ');
  }

  // URL state: auto-load if path is in query param
  var params = new URLSearchParams(window.location.search);
  var pathFromUrl = params.get('path');
  var tabFromUrl = params.get('tab');
  if (pathFromUrl) {
    document.getElementById('tablePath').value = pathFromUrl;
    document.getElementById('timelineForm').dispatchEvent(new Event('submit'));

    // Activate the requested tab after load
    if (tabFromUrl) {
      var tabMap = { config: 'tab-config', schema: 'tab-schema' };
      var targetTabId = tabMap[tabFromUrl];
      if (targetTabId) {
        // Wait for the timeline load to complete and DOM to update
        var observer = new MutationObserver(function () {
          if (!stateLoaded.classList.contains('d-none')) {
            observer.disconnect();
            var tabEl = document.getElementById(targetTabId);
            if (tabEl) {
              bootstrap.Tab.getOrCreateInstance(tabEl).show();
            }
          }
        });
        observer.observe(stateLoaded, { attributes: true, attributeFilter: ['class'] });
      }
    }
  }
})();
