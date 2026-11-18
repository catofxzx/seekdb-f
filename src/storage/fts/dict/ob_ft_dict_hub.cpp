/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ft_dict_hub.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/dict/ob_ft_cache_container.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_range_dict.h"
namespace oceanbase
{
namespace storage
{
int ObFTDictHub::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dict_map_.create(K_MAX_DICT_BUCKET, "dict_map"))) {
  } else if (OB_FAIL(rw_dict_lock_.init(K_MAX_DICT_BUCKET))) {
  } else if (OB_FAIL(parsed_dict_lock_.init(K_MAX_PARSED_DICT_CNT))) {
  } else {
    is_inited_ = true;
  }
  return ret;
};

int ObFTDictHub::destroy()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < K_MAX_PARSED_DICT_CNT; ++i) {
    ObFTParsedDict &parsed_dict = parsed_dicts_[i];
    if (OB_NOT_NULL(parsed_dict.dict)) {
      parsed_dict.dict->~ObIFTDict();
      allocator_.free(parsed_dict.dict);
      parsed_dict.dict = nullptr;
    }
    if (OB_NOT_NULL(parsed_dict.container)) {
      parsed_dict.container->~ObFTCacheRangeContainer();
      allocator_.free(parsed_dict.container);
      parsed_dict.container = nullptr;
    }
  }
  is_inited_ = false;
  return ret;
}
int ObFTDictHub::build_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  ObFTDictInfo info;
  container.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObBucketHashWLockGuard guard(rw_dict_lock_, key.hash());

    // try if valid with no recursive lock
    if (OB_FAIL(get_dict_info(key, info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // dict not exist, make new one, by caller
        ret = OB_ENTRY_NOT_EXIST;
      } else {
      }
    } else if (OB_FAIL(ObFTRangeDict::try_load_cache(desc, info.range_count_, container))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
      } else {
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(ObFTRangeDict::build_cache_from_ik_dict(desc, container))) {
        } else if (FALSE_IT(info.range_count_ = container.get_handles().size())) {
        } else if (OB_FAIL(put_dict_info(key, info))) {
        }
      }
    }
  }
  return ret;
}

int ObFTDictHub::load_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  ObFTDictInfo info;
  container.reset();
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    {
      ObBucketHashRLockGuard guard(rw_dict_lock_, key.hash());
      if (OB_FAIL(get_dict_info(key, info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // dict not exist, make new one, by caller
          ret = OB_ENTRY_NOT_EXIST;
        } else {
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObFTRangeDict::try_load_cache(desc, info.range_count_, container))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // dict not exist, make new one, by caller
      } else {
      }
    }
  }

  return ret;
}


int ObFTDictHub::get_parsed_dict(const ObFTDictDesc &desc, ObIFTDict *&dict)
{
  int ret = OB_SUCCESS;
  dict = nullptr;
  const uint32_t dict_type = static_cast<uint32_t>(desc.type_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(dict_type >= K_MAX_PARSED_DICT_CNT)) {
    ret = OB_NOT_SUPPORTED;
  } else {
    ObBucketHashWLockGuard guard(parsed_dict_lock_, dict_type);
    ObFTParsedDict &parsed_dict = parsed_dicts_[dict_type];
    if (OB_NOT_NULL(parsed_dict.dict)) {
      dict = parsed_dict.dict;
    } else {
      ObFTCacheRangeContainer *container = OB_NEWx(ObFTCacheRangeContainer, &allocator_, allocator_);
      if (OB_ISNULL(container)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        if (OB_FAIL(load_cache(desc, *container))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            // the dict is not in the cache yet, build it
            ret = OB_SUCCESS;
            if (OB_FAIL(build_cache(desc, *container))) {
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObFTRangeDict *range_dict = OB_NEWx(ObFTRangeDict, &allocator_, allocator_, container, desc);
          if (OB_ISNULL(range_dict)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (OB_FAIL(range_dict->init())) {
            range_dict->~ObFTRangeDict();
            allocator_.free(range_dict);
          } else {
            parsed_dict.container = container;
            parsed_dict.dict = range_dict;
            dict = range_dict;
          }
        }
        if (OB_FAIL(ret)) {
          container->~ObFTCacheRangeContainer();
          allocator_.free(container);
        }
      }
    }
  }
  return ret;
}

int ObFTDictHub::get_dict_info(const ObFTDictInfoKey &key, ObFTDictInfo &info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dict_map_.get_refactored(key, info))) {
    if (OB_HASH_NOT_EXIST != ret) {
    }
  }

  return ret;
}

int ObFTDictHub::put_dict_info(const ObFTDictInfoKey &key, const ObFTDictInfo &info)
{
  int ret = OB_SUCCESS;
  const int cover_exist_flag = 1;
  if (OB_FAIL(dict_map_.set_refactored(key, info, cover_exist_flag))) {
  }

  return ret;
}
} //  namespace storage
} //  namespace oceanbase
